import sys

sys.path.append("../load_source_data")

from upload_to_container import UploadToContainer

def fetch_data():
    utc = UploadToContainer()
    lf = utc.load_forecasts()
    utc.conn.commit()
    utc.conn.close()
    return lf