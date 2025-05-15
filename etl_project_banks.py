from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime



def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')  


def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) >= 3:
            data_dict = {
                "Name": col[1].text.strip(),
                "MC_USD_Billion": float(col[2].text.strip().replace('\n', '').replace(',', ''))
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            if not df1.empty:
                df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df, csv_path):
    exchange_rates = pd.read_csv(csv_path)
    exchange_rates.columns = exchange_rates.columns.str.strip()

    rates = exchange_rates.set_index('Currency')['Rate'].to_dict()
    df.columns = df.columns.str.strip()
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)

    df['MC_GBP_Billion'] = df['MC_USD_Billion'].apply(lambda x: round(x * rates['GBP'], 2))
    df['MC_EUR_Billion'] = df['MC_USD_Billion'].apply(lambda x: round(x * rates['EUR'], 2))
    df['MC_INR_Billion'] = df['MC_USD_Billion'].apply(lambda x: round(x * rates['INR'], 2))

    return df


def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
exchange_csv_path= './exchange_rate.csv'

log_progress("Preliminaries complete. Initiating ETL process")

df = extract(url, table_attribs)
print(df)

log_progress("Data extraction complete. Initiating Transformation process")

df = transform(df,exchange_csv_path)
print(df)
log_progress("Data transformation complete. Initiating Loading process")


load_to_csv(df, csv_path)
log_progress("Data saved to CSV file")

sql_connection= sqlite3.connect(db_name)
log_progress("SQL Connection initiated")

load_to_db(df, sql_connection, table_name)
log_progress("Data loaded to Database as a table, Executing queries")



query_statement = f"SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)

query_statement = f"SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)

log_progress("Process Complete")
sql_connection.close()
log_progress("Server Connection closed")