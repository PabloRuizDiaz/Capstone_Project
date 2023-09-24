import os
import pandas as pd
import sqlalchemy as sql


cities = ['Asheville','Austin','Bozeman','Broward County','Cambridge','Chicago','Clark County NV',
        'Columbus','Dallas','Denver','Fort Worth','Hawaii','Jersey City','Los Angeles','Nashville',
        'New Orleans','New York City','Newark','Oakland','Pacific Grove','Portland','Rhode Island',
        'Salem','San Diego','San Francisco','San Mateo County','Santa Clara County','Santa Cruz County',
        'Seattle','Twin Cities MSA','Washington DC']
cities = ['Asheville']
tables_name = ['calendar','listings','listings_detailed','neighbourhoods','reviews','reviews_detailed']

try:
    engine = sql.create_engine("mysql+pymysql://root:admin@localhost:3306/rawAirbnbDB")
    conn = engine.connect()
    print(f"Connection done to {engine} successful.")
except:
    print("Connection failed! Please check if database is working.")
    exit()

BASE_DIR = os.path.dirname(os.path.abspath('__file__'))

for city in cities:
    for table in tables_name:
        fileCSV = os.path.join(BASE_DIR, 'database', 'usa', city, f'{table}.csv')
        dfRow = pd.read_csv(fileCSV)
        
        dfRow.to_sql(f'{city}_{table}', con=conn, schema='rawAirbnbDB', if_exists='replace', index=False, chunksize=1000, method=None)

        print(f"{city}_{table} created!")

conn.close()
print("Connection closed.")
