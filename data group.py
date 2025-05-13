
import geopandas as gpd
import time
import pandas as pd
from sqlalchemy import create_engine
import requests

sa2_gdf = gpd.read_file(
    r".\SA2_2021_AUST_SHP_GDA2020\SA2_2021_AUST_SHP_GDA2020\SA2_2021_AUST_GDA2020.shp"
)

# 2. Set target SA4 code to "102" (Central Coast)
target_sa4 = "102"


sa2_in_sa4 = sa2_gdf[sa2_gdf['SA4_CODE21'] == target_sa4]
print(f"a total of {len(sa2_in_sa4)} SA2 area")
print(sa2_in_sa4[['SA2_CODE21', 'SA2_NAME21']].drop_duplicates())


def fetch_poi(minx, miny, maxx, maxy):
    url = "https://maps.six.nsw.gov.au/arcgis/rest/services/public/NSW_POI/MapServer/0/query"
    params = {
        "where": "1=1",
        "geometry": f"{minx},{miny},{maxx},{maxy}",
        "geometryType": "esriGeometryEnvelope",
        "spatialRel": "esriSpatialRelIntersects",
        "inSR": 4283,
        "outFields": "*",
        "outSR": 4283,
        "f": "json"
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


records = []
for _, row in sa2_in_sa4.iterrows():
    minx, miny, maxx, maxy = row.geometry.bounds
    poi_json = fetch_poi(minx, miny, maxx, maxy)
    for feat in poi_json.get('features', []):
        attr, geom = feat.get('attributes'), feat.get('geometry')
        records.append({
            'name': attr.get('NAME'),
            'category': attr.get('CATEGORY'),
            'longitude': geom.get('x'),
            'latitude': geom.get('y'),
            'sa2_code': row['SA2_CODE21']
        })
    time.sleep(1)


engine = create_engine("postgresql+psycopg2://root:123456@localhost:5432/poi_data")

df = pd.DataFrame(records)
if not df.empty:
    df.to_sql('poi_data', engine, if_exists='append', index=False, chunksize=500)
    print(f"Successfully wrote {len(df)} records to the database")
else:
    print("No POI data was obtained and not written to the database.")
