import os
import pandas as pd
import sqlalchemy as sql


cities = ['Asheville','Austin','Bozeman','Broward County','Cambridge','Chicago','Clark County NV',
        'Columbus','Dallas','Denver','Fort Worth','Hawaii','Jersey City','Los Angeles','Nashville',
        'New Orleans','New York City','Newark','Oakland','Pacific Grove','Portland','Rhode Island',
        'Salem','San Diego','San Francisco','San Mateo County','Santa Clara County','Santa Cruz County',
        'Seattle','Twin Cities MSA','Washington DC']

states = ['North Carolina','Texas','Montana','Florida','Massachusetts','Illinois','Nevada',
        'Ohio','Texas','Colorado','Texas','Hawaii','New Jersey','California','Tennessee',
        'Louisiana','New York','New Jersey','California','California','Oregon','Rhode Island',
        'Oregon','California','California','California','California','California',
        'Washington','Minnesota','Washington']

tables_name = ['listings_detailed','reviews_detailed','calendar']

try:
    engine = sql.create_engine("mysql+pymysql://root:admin@localhost:3306/AirbnbDB")
    conn = engine.connect()
    print(f"Connection done to {engine} successful.")
except:
    print("Connection failed! Please check if database is working.")
    exit()

BASE_DIR = os.path.dirname(os.path.abspath('__file__'))

for index, city in enumerate(cities):
    for table in tables_name:
        fileCSV = os.path.join(BASE_DIR, 'database', 'usa', city, f'{table}.csv')
        dfRow = pd.read_csv(fileCSV)
        
        if table == "listings_detailed":
            dfRow['city'] = city
            dfRow['state'] = states[index]

            dfRow['bathrooms'] = dfRow['bathrooms_text'].str.extract('([-+]?\d*\.?\d+)')
            dfRow['bathrooms_type'] = dfRow['bathrooms_text'].replace('([-+]?\d*\.?\d+)', '', regex=True)

            dfRow['price'] = dfRow['price'].str.extract('([-+]?\d*\.?\d+)')

            dfRow.rename(columns={'number_of_reviews_ltm':'number_of_reviews_l12m'}, inplace=True)

            dfRow = dfRow[['id', 'price', 'minimum_nights', 'maximum_nights', 'name', 'description', 'host_id', 'latitude',
                        'longitude', 'property_type', 'room_type', 'accommodates', 'bathrooms',
                        'bathrooms_type', 'bedrooms', 'beds' , 'amenities', 'number_of_reviews',
                        'number_of_reviews_l12m', 'number_of_reviews_l30d', 'first_review', 'last_review',
                        'review_scores_rating', 'review_scores_accuracy', 'review_scores_cleanliness', 'review_scores_checkin',
                        'review_scores_communication', 'review_scores_location', 'review_scores_value', 'city', 'state']]
            
            dfRow.astype({'id':'Int64', 'price':'float64', 'minimum_nights':'Int64', 'maximum_nights':'Int64', 'name':'object', 
                          'description':'object', 'host_id':'Int64', 'latitude':'float64',
                        'longitude':'float64', 'property_type':'object', 'room_type':'object', 'accommodates':'Int64', 'bathrooms':'float64',
                        'bathrooms_type':'object', 'bedrooms':'Int64', 'beds':'Int64', 'amenities':'object', 'number_of_reviews':'Int64',
                        'number_of_reviews_l12m':'Int64', 'number_of_reviews_l30d':'Int64', 'first_review':'object', 'last_review':'object',
                        'review_scores_rating':'float64', 'review_scores_accuracy':'float64', 'review_scores_cleanliness':'float64', 'review_scores_checkin':'float64',
                        'review_scores_communication':'float64', 'review_scores_location':'float64', 'review_scores_value':'float64', 'city':'object', 'state':'object'})

        elif table == "calendar":
            dfRow = dfRow[dfRow.available == 'f']
            dfRow['price2'] = dfRow['price'].str.extract('([-+]?\d*\.?\d+)')
            
            dfRow = dfRow[['listing_id', 'date', 'available', 'price2', 'minimum_nights', 'maximum_nights']]

            dfRow.rename(columns={'price2':'price'}, inplace=True)

            dfRow.astype({'listing_id':'Int64', 'date':'object', 'available':'object', 'price':'float64', 'minimum_nights':'Int64', 'maximum_nights':'Int64'})

        elif table == "reviews_detailed":
            dfRow = dfRow[['listing_id', 'date', 'reviewer_id', 'reviewer_name', 'comments']]

            dfRow.astype({'listing_id':'Int64', 'date':'object', 'reviewer_id':'Int64', 'reviewer_name':'object', 'comments':'object'})
            
        dfRow.to_sql(f'{table}', con=conn, schema='AirbnbDB', if_exists='append', index=False, chunksize=1000, method=None)

        print(f"{city}_{table} appended to {engine}!")

conn.close()
print("Connection closed.")
