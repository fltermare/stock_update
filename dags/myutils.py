import datetime
import json
import numpy as np
import pandas as pd
import requests
import psycopg2.extras


def query_yahoo_finance(stock_code, start, execution_date):

    # convert execution_date to timestamp
    execution_date = execution_date.format("%Y-%m-%d")
    element = datetime.datetime.strptime(execution_date,"%Y-%m-%d")
    end = int(datetime.datetime.timestamp(element))

    site = "https://query1.finance.yahoo.com/v8/finance/chart/{stock_code:s}?period1={start:d}&period2={end:d}&interval=1d&events=history".format(
                stock_code=stock_code, start= start, end=end)

    response = requests.get(site)
    data = json.loads(response.text)
    df = pd.DataFrame(data['chart']['result'][0]['indicators']['quote'][0],
                      index=pd.to_datetime(np.array(data['chart']['result'][0]['timestamp'])*1000*1000*1000))
    df['date'] = df.index.strftime("%Y-%m-%d")
    df['stock_code'] = stock_code

    return df.dropna()


def check_stock_day_exist(stock_code, execution_date, config):

    # create database connection
    conn = config.db_conn()
    cursor = conn.cursor()

    query_sql = """
        SELECT EXISTS(
            SELECT 1
            FROM history as h
            WHERE h.stock_code = %(stock_code)s and h.date = %(date)s
        );
    """

    execution_date = execution_date.format("%Y-%m-%d")
    cursor.execute(query_sql, {'stock_code': stock_code, 'date': execution_date})
    result = cursor.fetchone()
    print('[check_stock_day_exist.month_record]', execution_date, result)

    # close database connection
    cursor.close()
    conn.close()

    return result[0]


def insert_new_data(stock_code, df, config):

    # create database connection
    conn = config.db_conn()
    cursor = conn.cursor()

    table = 'history'
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))

    insert_sql = """
        INSERT INTO {table:s}({cols:s})
        VALUES %s
        ON CONFLICT (stock_code, date)
        DO NOTHING;
    """.format(table=table, cols=cols)

    try:
        psycopg2.extras.execute_values(cursor, insert_sql, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        raise

    # close database connection
    cursor.close()
    conn.close()
