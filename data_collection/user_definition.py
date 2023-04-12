from datetime import date, datetime, timedelta

yesterday = date.today() - timedelta(days=1)
two_days_ago = date.today() - timedelta(days=2)
three_days_ago = date.today() - timedelta(days=3) 

data_dir = "/Users/tejashree/Desktop/USF/Distributed Data Systems/2023-msds697-distributed-data-systems/Day1/Example1/data"

sf_data_url = "data.sfgov.org"
data_limit = 200000
sf_data_sub_uri = "gnap-fj3t"
sf_data_app_token = "6TIsqkC2niNuQD8bnsBng7Kle"

noaa_token = "MkmbgeVKxdIuuelceXKyNOcaFCIDuxEc"
station_id = "GHCND:USW00023272"
dataset_id = "GHCND"
location_id = "CITY:US060031"
noaa_endpoint = f"data?datasetid={dataset_id}&datatypeid=PRCP&station_id={station_id}&startdate={three_days_ago}&enddate={two_days_ago}"
noaa_api_url = f"https://www.ncei.noaa.gov/cdo-web/api/v2/{noaa_endpoint}"