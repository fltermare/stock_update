import os
import psycopg2
import pandas as pd
import requests


class Setting:
    def __init__(self, host='db'):
        self.host = host
        self.stock_code_list = self.select_all_stock_codes()        

    def db_conn(self):
        conn = psycopg2.connect(
            host=self.host,
            # host='localhost',
            database='db',
            user='postgres',
            password='postgres'
        )

        return conn

    def select_all_stock_codes(self):
        conn = self.db_conn()
        para_p_sql = """
            SELECT DISTINCT s.stock_code
            FROM stock as s;
        """
        df = pd.read_sql(para_p_sql, con=conn)
        conn.close()
        # return ['0050']
        # return ['0050', '0056']
        return sorted(df['stock_code'].values)

# a = Setting()
# print(a.stock_code_list)