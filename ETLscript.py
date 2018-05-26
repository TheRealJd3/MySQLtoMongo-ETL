import pymysql  #
from pymongo import MongoClient
import copy

DB_EXISTS = True  #initially was false now true because db exists after testing
display = True # Print oon screen to know process being done during pipeline process
def initalise_mongo():
    return MongoClient("localhost", 27017)["test"]   # MongoClient ( host,port no.(default is 27017)[dbname]


def initalise_mysql():
    #Initialize mysql with given parameters
    return pymysql.connect(
        host="localhost",
        user="root",
        password="",
        db="cw"
    )

def sql_query(sql, cursor, query_type):
    #Function that will be used to carry out queries
    if query_type == "fetchall":
        #http://pymysql.readthedocs.io/en/latest/modules/cursors.html
        #documetnation was used
        cursor.execute(sql)
        return cursor.fetchall()
    else:
        print("fetchall only try again ")

#With use of a sql cursor,Extract (E from ETL)
#data from db provided
def extract_sqldata(sql_cursor):
    #http://pymysql.readthedocs.io/en/latest/user/examples.html
    #instead of individual querying i made a method for it
    cities = sql_query('select * from cities', sql_cursor, 'fetchall')
    countries = sql_query('select * from countries', sql_cursor, 'fetchall')
    airports = sql_query('select * from airports', sql_cursor, 'fetchall')
    airlines = sql_query('select * from airlines', sql_cursor, 'fetchall')
    routes = sql_query('select * from routes', sql_cursor, 'fetchall')
    data = (cities, countries, airports, airlines, routes)
    return data

#Source https://gis.stackexchange.com/questions/116655/replacing-
# null-value-with-zero-in-geodatabase-table-using-python-parser-of-arcgi
def replaceNull(x):
  if x is None:
      return ""
  elif x is "Null":
      return ""
  else:
    return x

def replaceNullInt(x):
    if x is None:
        return 0
    elif x is "Null":
        return None
    elif x is "null":
        return None
    else:
        return x

#Function for T of ETL(Transform)
#Here the transformation of the sql sqltables to relevant json schema
#as will be mentioned in report will be done
def transform_to_mongo(collection,sqltable):
    collection_test = []
    sqltable_data = { }
    if sqltable == "routes":
        for item in collection[4]:
            sqltable_data['alid'] = replaceNullInt(item[0])
            sqltable_data['src_ap'] = replaceNull(item[1])
            sqltable_data['src_apid'] = replaceNullInt(item[2])
            sqltable_data['dst_ap'] = replaceNull(item[3])
            sqltable_data['dst_apid'] = replaceNullInt(item[4])
            sqltable_data['codeshare'] = replaceNull(item[5])
            sqltable_data['stops'] = replaceNullInt(item[6])
            sqltable_data['equipment'] = replaceNull(item[7])
            sqltable_data['rid'] = replaceNullInt(item[8])
            collection_test.append(copy.copy(sqltable_data)) #https://www.python-course.eu/deep_copy.php reference
            #appeneded
        return collection_test
    elif sqltable == "cities":
        for item in collection[0]:
            sqltable_data['cid'] = replaceNullInt(item[0])
            sqltable_data['name'] = replaceNull(item[1])
            sqltable_data['country'] = replaceNull(item[2])
            sqltable_data['timezone'] = replaceNullInt(item[3])
            sqltable_data['tz_id'] = replaceNull(item[4])
            airports_collection=[]
            tempdb={}
            for airportitem in collection[2]:
                #new airport object
                if airportitem[1]==sqltable_data['cid']:
                    tempdb['apid']=replaceNullInt(airportitem[7])
                    tempdb['name']= replaceNull(airportitem[0])
                    tempdb['iata']=replaceNull(airportitem[2])
                    tempdb['icao']=replaceNull(airportitem[3])
                    tempdb['x']=replaceNullInt(airportitem[4])
                    tempdb['y'] = replaceNullInt(airportitem[5])
                    tempdb['elevation'] = replaceNull(airportitem[6])
                    airports_collection.append(copy.copy(tempdb))
            sqltable_data['airport'] = airports_collection #add as airport
            collection_test.append(copy.copy(sqltable_data))
        return collection_test
    elif sqltable == "countries":
        for item in collection[1]:
            sqltable_data['code'] = replaceNull(item[0])
            sqltable_data['name'] = replaceNull(item[1])
            sqltable_data['oa_code'] = replaceNull(item[2])
            sqltable_data['dst'] =replaceNull(item[3])
            airlines_collection=[]
            secondtest={}
            for airlineitem in collection[3]:
                if airlineitem[4]==sqltable_data['code']:
                    #matching tables
                    secondtest['alid'] = replaceNullInt(airlineitem[5])
                    secondtest['name'] = replaceNull(airlineitem[0])
                    secondtest['iata'] = replaceNull(airlineitem[1])
                    secondtest['icao'] = replaceNull(airlineitem[2])
                    secondtest['callsign'] = replaceNull(airlineitem[3])
                    secondtest['alias'] = replaceNull(airlineitem[6])
                    secondtest['mode'] = replaceNull(airlineitem[7])
                    secondtest['active'] = replaceNull(airlineitem[8])
                    airlines_collection.append(copy.copy(secondtest))
            sqltable_data['airline'] = airlines_collection
            collection_test.append(copy.copy(sqltable_data))
        return collection_test


#Loads transfromed sqldata into mongo db( L of ETL)
def load_data(mongo_collection, collection_test):
    if DB_EXISTS: #mentioned earlier flag to check if country exists
        mongo_collection.delete_many({})
    else:
        pass
    return mongo_collection.insert_many(collection_test)


def main():
    #starts pipeline
    if display:
        print('Starting automated data pipeline for ETL data from mysql to mongodb')
        print('Initialising MySQL connection')
    mysql = initalise_mysql()

    if display:
        print('MySQL connected')
        print('Starting data pipeline stage 1 : Extracting data from MySQL')
    mysql_cursor = mysql.cursor()
    mysql_data = extract_sqldata(mysql_cursor)

    if display:
        print('Stage 1 completed! Data successfully extracted from MySQL')
        print('Starting data pipeline stage 2: Transforming data from MySQL for MongoDB')
        print('Transforming routes dataset')
    routes_collection = transform_to_mongo(mysql_data, "routes")
    if display:
        print('Successfully transformed routes now cities')
        print('Transforming cities')
    cities_collection = transform_to_mongo(mysql_data, "cities")
    if display:
        print('Successfully transformed cities now countries')
        print('Transforming countries')
    countries_collection = transform_to_mongo(mysql_data, "countries")


    if display:
        print('Successfully transformed cities now init mongo')
        print('Data successfully transformed')
        print('Intialising MongoDB connection')
    mongo = initalise_mongo()

    if display:
        print('MongoDB connection successfully')
        print('Loading transformed data into mongo')
    result = load_data(mongo['routes'], routes_collection)
    if display:
        print('Successfully loaded routes now cities')
        print('Loading cities')
    result = load_data(mongo['cities'], cities_collection)
    if display:
        print('Successfully loaded routes now cities')
        print('Loading cities')
    result = load_data(mongo['countries'], countries_collection)




    if display:
        print('Stage 3 completed! Data successfully loaded')
        print('Closing MySQL connection')
    mysql.close()
    if display:
        print('MySQL connection closed successfully')
        print('Ending data pipeline')

main()