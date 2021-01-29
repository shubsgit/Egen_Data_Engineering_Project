#!/usr/bin/env python
# coding: utf-8

# In[27]:


import warnings
warnings.filterwarnings("ignore")

#loading data 
import requests as rq
import pandas as pd
import re

# Date format
from datetime import datetime

# Database Connection
import sqlite3
from sqlite3 import Error

#for multiprocessing
from multiprocessing import Pool

import concurrent.futures


# get data from the Json API        
def get_daily_data(api_url):
    json_data = rq.get(api_url)
    json_data=json_data.json()
    return json_data


# convert json data into dataframe
def convert_to_df(json_data):
    columns=json_data["meta"]["view"]["columns"]
    col_names = []

    for i in range(len(columns)):
        name = columns[i]["fieldName"].strip()
        col_names.append(name.replace(":",""))

    data_rows = json_data["data"]

    df=pd.DataFrame(data_rows,columns=col_names)

    return df

    


# fixing irregularities in the county names and creating a list of all counties
def county_cleaner(counties):
    ans=[]
    for i in counties:

    # Remove all non-word characters (everything except numbers and letters)
        i = re.sub(r"[^\w\s]", '', i)

    # Replace all runs of whitespace with a single dash
        i = re.sub(r"\s+", '_', i)

        ans.append(i)
    return ans

# fixing irregularities in a single county name
def single_county_cleaner(county):

    # Remove all non-word characters (everything except numbers and letters)
    county = re.sub(r"[^\w\s]", '', county)

    # Replace all runs of whitespace with a single dash
    county = re.sub(r"\s+", '_', county)

    return county

# creating connection to the database
def create_connection():
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect('egen.db')
        return conn
    except Error as e:
        print(e)

    return conn

# creating a table inside the database 
def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
        conn.commit()
    except Error as e:
        print(e)


def update_tables(df1):
#     create connection
    conn=create_connection()
    c=conn.cursor()
#     get county name
    county_name = single_county_cleaner(df1.county.values[0])
    df1.drop("county", axis = 1,inplace=True)
    
#     find the last date the table was updated
    c=conn.cursor()    
    last_date_query = f'SELECT test_date FROM Albany ORDER BY test_date DESC LIMIT 1'
    c.execute(last_date_query)
    last_date=c.fetchall()

#   if last date is present, filter the dataframe to insert only latest values
    if len(last_date) != 0:
            df_new= df1[df1.test_date>pd.to_datetime(last_date[0][0])]
            df_new.to_sql(county_name,conn,if_exists='append',index=False)
#   else enter all  the rows inside the table          
    else:
        df1.to_sql(county_name,conn,if_exists='append',index=False)

# commit and close the connection
    conn.commit()
    conn.close()

    return f"Successfully updated {county_name} table"


def main():
#         for the given API, fetch the data
        api_url="https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD"
        json_data = get_daily_data(api_url)
        df=convert_to_df(json_data)

    #     selecting only interested columns
        df1=df[['county','test_date','new_positives','cumulative_number_of_positives','total_number_of_tests','cumulative_number_of_tests']]
        df1.insert(0, 'load_date',datetime.today().strftime('%Y-%m-%d') )
        df1["test_date"]=pd.to_datetime(df1["test_date"])
        df1.index = df1.index + 1
        counties_list=county_cleaner(df1.county.unique())
        
#         create a list of SQL scripts
        create_SQL_scripts=[]
        for county in counties_list:
            sql_create_tasks_table = f"""CREATE TABLE IF NOT EXISTS {county} (
                                        test_date TEXT NOT NULL,
                                        new_positives INTEGER NOT NULL,
                                        cumulative_number_of_positives INTEGER,
                                        total_number_of_tests INTEGER NOT NULL,
                                        cumulative_number_of_tests INTEGER NOT NULL,
                                        load_date TEXT NOT NULL 
                                    );"""
            create_SQL_scripts.append(sql_create_tasks_table)

        # create a database connection
        conn = create_connection()

        # create tables
        if conn is not None:
            for create_script in create_SQL_scripts:
                create_table(conn, create_script)   
        else:
            print("Error! cannot create the database connection.")


#       with the help of a multithreaded executor, passing on chuncks of data to load into the database
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            df_county_list = [pd.DataFrame(y) for x, y in df1.groupby('county', as_index=True)]
            future_objects=[executor.submit(update_tables,frame) for frame in df_county_list]


            for f in concurrent.futures.as_completed(future_objects):
                print(f.result())


        conn.commit()
        conn.close()

        
if __name__ == '__main__':
    main()


# In[ ]:




