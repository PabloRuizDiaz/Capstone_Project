import pandas as pd
import sqlalchemy as sql
from sqlalchemy import text
from sqlalchemy.types import *
import re
import nltk
from nltk import stem
from nltk.corpus import stopwords
import collections


df_schema = {
    "token": Text,
    "frequency": Integer
}


class textMining:
    def __init__(self, table, column, by_city=None):
        """
        Method that runs at the first time when the object is created. Here the object is able to
        connect to a database (DBMS: MySQL) creating the engine and connection. Finally it creates
        the dataframe with specific text column.
        
        Arguments:
            > table: string, name of the table to connect to the database;
            > column: string, name of the column to tokenize;
            > by_city: string, name of the city to filter the data from the database.
        
        Output:
            > dataframe with one column.
        """
        try:
            self._engine = sql.create_engine("mysql+pymysql://root:admin@localhost:3306/AirbnbDB")
            self._conn = self._engine.connect()
            print(f"Connection done to {self._engine} successful.")
        except:
            print("Connection failed! Please check if database is working.")
            exit()
        
        print(f"Reading the following data: {table} -> column: {column}")
        if by_city == None:
            query = f"SELECT {column} FROM {table}" 
            if_exists = 'replace'
            
        else:
            query = f"""SELECT rd.{column} FROM reviews_detailed rd
                    INNER JOIN listings_detailed ld ON rd.listing_id = ld.id 
                    WHERE ld.city = '{by_city}'"""
            if_exists = 'append'

        query = text(query)
        df = pd.read_sql(query, self._conn)
    
        self.allTokens = self.tokenize(df[f"{column}"].dropna(), isStemmed=False)
        
        self.getAllTokes(table, column, if_exists=if_exists, by_city=by_city)
        
        # Closing the connection to the database
        self._conn.close()
        print("Connection closed.")

    def tokenize(self, corpusList, isStemmed=False):
        """
        Method that runs across each element in a Serie with strings, and tokenze them (split all elements by the space
        and dropped any which it is not an alphanumeric element or an stopWord).
        
        Arguments:
            > corpusList: serie, serie of strings;
            > isStemmed (default=False): boolean, True if the strings needs to be stemmed.
        
        Output:
            > finalTokensFiltered: list, list of strings.
            
        Note:
            > tokens are find by a regular expression '\b\w+\b';
            > StopWord are from the method getStopWords().
        """
        
        if not hasattr(textMining, 'stopWords'):
            self.getStopWords()
        
        finaltokens = []

        for index, value in corpusList.items():
            text = value.lower().strip()
            tokens = re.findall(r'\b\w+\b', text)

            tokens_filtered = [token for token in tokens if token not in self.stopWords]

            for x in tokens_filtered:
                finaltokens.append(x)

        if isStemmed:
            porterstem = stem.PorterStemmer()
            finaltokens = [porterstem.stem(x) for x in finaltokens]

        return finaltokens

    def getStopWords(self):
        """
        Method that generates a set of words (class variable) using the library nltk. A stop word is a commonly used word 
        (such as “the”, “a”, “an”, “in”) that a search engine has been programmed to ignore. 
        
        Arguments:
            > None.
        
        Output:
            > None.
        """
        nltk.download('stopwords')
        stopWords = set(stopwords.words('english'))
        stopWords.update({'would', 'could'})
        self.stopWords = stopWords

    def getAllTokes(self, table, column, if_exists, by_city):
        """
        Method to upload dataframe to the database creating a table with two columns; 
        1) word, and 2) number of repetitions.
        
        Arguments:
            > table: string, name of the table to connect to the database;
            > column: string, name of the column to tokenize;
            > if_exists: string, how to behave if the table already exists;
            > by_city: string, name of the city to add in the table.
        
        Output:
            > None.
        """
        frequencyDict = dict(collections.Counter(self.allTokens))
        df = pd.DataFrame.from_dict(frequencyDict, orient='index').reset_index()
        df.rename(columns={'index':'token', 0:'frequency'}, inplace=True)

        if by_city is not None:
            df['city'] = by_city

        df.to_sql(
                f'{table}_{column}_tokens', 
                con=self._engine, 
                schema='AirbnbDB', 
                if_exists=if_exists, 
                index=False, 
                chunksize=1000, 
                method=None, 
                dtype=df_schema)


if __name__=='__main__':
    textMining(table='listings_detailed', column='name')
    textMining(table='listings_detailed', column='description')
    textMining(table='listings_detailed', column='property_type')
    textMining(table='listings_detailed', column='amenities')
    
    cities = ['Asheville','Austin','Bozeman','Broward County','Cambridge','Chicago','Clark County NV',
        'Columbus','Dallas','Denver','Fort Worth','Hawaii','Jersey City','Los Angeles','Nashville',
        'New Orleans','New York City','Newark','Oakland','Pacific Grove','Portland','Rhode Island',
        'Salem','San Diego','San Francisco','San Mateo County','Santa Clara County','Santa Cruz County',
        'Seattle','Twin Cities MSA','Washington DC']
    
    for city in cities:
        textMining(table='reviews_detailed', column='comments', by_city=city)