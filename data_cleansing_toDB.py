import sqlalchemy as sql
import pickle
import pandas as pd


try:
    engine = sql.create_engine("mysql+pymysql://root:admin@localhost:3306/AirbnbDB")
    conn = engine.connect()
    print(f"Connection done to {engine} successful.")
except:
    print("Connection failed! Please check if database is working.")
    exit()

print('### Working with listings_detailed table. ###')

conn.execute(sql.text("DELETE FROM listings_detailed WHERE price < 20"))
print("DELETE FROM listings_detailed WHERE price < 20... DONE!")

conn.execute(sql.text("DELETE FROM listings_detailed WHERE minimum_nights > maximum_nights"))
print("DELETE FROM listings_detailed WHERE minimum_nights > maximum_nights... DONE!")

conn.execute(sql.text("DELETE FROM listings_detailed WHERE minimum_nights > 180"))
print("DELETE FROM listings_detailed WHERE minimum_nights > 180... DONE!")

conn.execute(sql.text("DELETE FROM listings_detailed WHERE bathrooms > 5"))
print("DELETE FROM listings_detailed WHERE bathrooms > 5... DONE!")

conn.execute(sql.text("DELETE FROM listings_detailed WHERE bedrooms > 16"))
print("DELETE FROM listings_detailed WHERE bedrooms > 16... DONE!")

conn.execute(sql.text("DELETE FROM listings_detailed WHERE beds > 16"))
print("DELETE FROM listings_detailed WHERE beds > 16... DONE!")

conn.execute(sql.text("DELETE FROM listings_detailed WHERE isnull(first_review)"))
print("DELETE FROM listings_detailed WHERE isnull(first_review)... DONE!")

conn.execute(sql.text("""
             DELETE FROM listings_detailed 
             WHERE 
                isnull(first_review) OR
                isnull(name) OR
                isnull(beds) OR
                isnull(description) OR
                isnull(review_scores_value) OR
                isnull(review_scores_location) OR
                isnull(review_scores_checkin) OR
                isnull(review_scores_accuracy) OR
                isnull(review_scores_communication) OR
                isnull(review_scores_cleanliness) OR
                isnull(bathrooms) OR
                isnull(bathrooms_type)
             """))
print("""DELETE FROM listings_detailed 
             WHERE 
                isnull(first_review) OR
                isnull(name) OR
                isnull(beds) OR
                isnull(description) OR
                isnull(review_scores_value) OR
                isnull(review_scores_location) OR
                isnull(review_scores_checkin) OR
                isnull(review_scores_accuracy) OR
                isnull(review_scores_communication) OR
                isnull(review_scores_cleanliness) OR
                isnull(bathrooms) OR
                isnull(bathrooms_type)... DONE!""")

print('Applying model to complete missing values from bedroom column.')
query = f"SELECT id, price, accommodates, bathrooms, beds FROM listings_detailed_raw WHERE isnull(bedrooms)"
df = pd.read_sql(query, con=engine)

model = pickle.load(open('bedrooms_nulls_model.pkl','rb'))

for index, serie in df.iterrows():
    feactures = pd.array([serie['price'], serie['accommodates'], serie['bathrooms'], serie['beds']])
    prediction = model.predict(feactures)

    query = f"INSERT INTO listings_detailed (bedrooms) VALUES (?) WHERE id = {serie['id']};"
    conn.execute(query, prediction[0])

print('Applying model to complete missing values from bedroom column... DONE!')


print('### Working with reviews_detailed table. ###')

# query = f"SELECT * FROM reviews_detailed"
# pd.read_sql(query, con=conn)


print('### Working with calendar table. ###')

# query = f"SELECT * FROM calendar"
# dfRow_ca = pd.read_sql(query, con=conn)

conn.commit()
conn.close()
print("Connection closed.")