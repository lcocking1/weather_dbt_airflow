import json
# from azure.cosmos import CosmosClient, PartitionKey
from get_weather_data import GetWeatherData
# import datetime as dt
import psycopg2
# import psycopg2.extras

class UploadToContainer:
    def __init__(self) -> None:
        c = open("access_control.json")
        cred = json.load(c)
        cred = cred["postgres"]
        
        self.database_name = "weather_warehouse_dev"
        conn_string = f"host={cred['host']} user={cred['user']} dbname={cred['dbname']} password={cred['password']} sslmode={cred['sslmode']} port={str(cred['port'])}"
        self.conn = psycopg2.connect(conn_string)
        self.cur = self.conn.cursor()
        self.cur.execute("set search_path = weather_ingestion;")
        self.weather_client = GetWeatherData()
        return None
    
    def load_forecasts(self):
        ''' load hourly forecasts for gridpoints in "gridpoints" container '''
        error_list = []       
        # get gridpoints ref data to call hourly forecast
        self.cur.execute("select gridpoints_key, grid_id, grid_x, grid_y from gridpoints;")
        data = self.cur.fetchall()
        
        for d in data:
            grid_data = self.weather_client.download_file(f"gridpoints/{d[1]}/{d[2]},{d[3]}/forecast/hourly")
            if 'error' in grid_data:
                error_list.append(grid_data)
                continue
            # insert data to grid_forecast table
            properties = grid_data["properties"]
            updated = self.weather_client.convert_to_datetime(properties["updated"])
            generated_at = self.weather_client.convert_to_datetime(properties["generatedAt"])
            
            for row in properties["periods"]:
                temp_dict = self.convert_to_columns(row)
                self.cur.execute(
                    """
                    insert into weather_ingestion.grid_forecast
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    on conflict (grid_forecast_key)
                        do update set
                            updated = %s,
                            generated_at = %s,
                            number = %s,
                            start_time = %s,
                            end_time = %s,
                            is_daytime = %s,
                            temperature = %s,
                            probability_of_precipitation = %s,
                            dewpoint = %s,
                            relative_humidity = %s,
                            wind_speed = %s,
                            wind_direction = %s,
                            short_forecast = %s
                    """, (
                        '_'.join([d[0], "{:03d}".format(row["number"])]),
                        d[0],
                        updated,
                        generated_at,
                        temp_dict['number'],
                        temp_dict['start_time'],
                        temp_dict['end_time'],
                        temp_dict['is_daytime'],
                        temp_dict['temperature'],
                        temp_dict['probability_of_precipitation'],
                        temp_dict['dewpoint'],
                        temp_dict['relative_humidity'],
                        temp_dict['wind_speed'],
                        temp_dict['wind_direction'],
                        temp_dict['short_forecast'],
                        updated,
                        generated_at,
                        temp_dict['number'],
                        temp_dict['start_time'],
                        temp_dict['end_time'],
                        temp_dict['is_daytime'],
                        temp_dict['temperature'],
                        temp_dict['probability_of_precipitation'],
                        temp_dict['dewpoint'],
                        temp_dict['relative_humidity'],
                        temp_dict['wind_speed'],
                        temp_dict['wind_direction'],
                        temp_dict['short_forecast']
                    )
                )
        return error_list

    def convert_to_columns(self, row):
        ''' convert to format required for table insert '''
        insert_dict = {}
        # insert_dict["grid_forecast_key"] = '_'.join([d[0], "{:03d}".format(row["number"])]) # period number in "001" format
        # insert_dict["gridpoints_key"] = d[0]
        # insert_dict["updated"] = updated
        # insert_dict["generated_at"] = generated_at
        insert_dict["number"] = row["number"]
        insert_dict["start_time"] = self.weather_client.convert_to_datetime(row["startTime"])
        insert_dict["end_time"] = self.weather_client.convert_to_datetime(row["endTime"])
        insert_dict["is_daytime"] = row["isDaytime"]
        insert_dict["temperature"] = row["temperature"]
        insert_dict["probability_of_precipitation"] = row["probabilityOfPrecipitation"]["value"]
        insert_dict["dewpoint"] = row["dewpoint"]["value"]
        insert_dict["relative_humidity"] = row["relativeHumidity"]["value"]
        insert_dict["wind_speed"] = row["windSpeed"].split(' ')[0]
        insert_dict["wind_direction"] = row["windDirection"]
        insert_dict["short_forecast"] = row["shortForecast"]
        return insert_dict