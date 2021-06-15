import requests
import numpy as np
import pandas as pd
import psycopg2
import time
from datetime import datetime, timedelta
import os


def run():
    print('starting cron....')
    start = time.time()
    df = get_dataframe()
    load_data_sql(df)
    end = time.time()
    print('execution time of script: ', str(timedelta(seconds=(end - start))))


def page_count():
    flag = True
    page_num = 1
    while flag:
        r = requests.get('https://www.adika.com/collections/picks/products.json?page={}'.format(page_num))
        data = r.json()
        yield data
        if len(data['products']) == 0:
            flag = False
        else:
            page_num += 1


def get_dataframe():
    df = []
    for page in page_count():
        for item in page['products']:
            tags = item['tags']
            updated_at = item['updated_at']
            title = item['title']
            handle = item['handle']
            for variant in item['variants']:
                sku = variant['sku']
                price = variant['price']
                compare_at_price = variant['compare_at_price']
                available = variant['available']
                df.append([sku, handle, title, price, compare_at_price, available, tags, updated_at])

    df = pd.DataFrame(df)
    df = df.rename(columns={0: 'sku', 1: 'handle', 2: 'title', 3: 'price', 4: 'compare_at_price', 5: 'available', 6: 'tags', 7: 'updated_at'})
    df['sku'].replace('', np.nan, inplace=True)
    df['compare_at_price'].fillna(0, inplace=True)
    df.dropna(subset=['sku'], inplace=True)
    df['campaign'] = 'influencers ' + datetime.now().strftime("%m-%d-%Y")
    df = df[['campaign', 'sku', 'handle', 'title', 'price', 'compare_at_price', 'available', 'tags', 'updated_at']]
    df = df.iloc[df.astype(str).drop_duplicates().index]
    print(df.tail())
    print(df.dtypes)
    print('number of rows and columns recorded in the data: ', df.shape)

    return df


def load_data_sql(df):
    connection = 0
    try:
        connection = psycopg2.connect(user=os.getenv('USER'),
                                      password=os.environ.get('PASSWORD'),
                                      host="",
                                      port="",
                                      database="")
        cursor = connection.cursor()
        truncate_query = "TRUNCATE TABLE analysis;"
        cursor.execute(truncate_query)
        connection.commit()
        print("truncated table before new insert batch")

        records_to_insert = list(df.itertuples(index=False, name=None))

        print('begin insert of data to table')

        cursor = connection.cursor()
        args_strings = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)", x).decode('utf-8') for x in records_to_insert)
        sql_insert_query = "INSERT INTO analysis VALUES " + args_strings
        cursor.execute(sql_insert_query)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into table")

    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into table {}".format(error))

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")