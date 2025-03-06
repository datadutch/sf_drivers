import snowflake.connector
import json
import pandas as pd

with open('config.json','r') as file:
    data = json.load(file)
    user = data['user']
    password = data['password']
    account = data['account']

con = snowflake.connector.connect(
    user = user,
    password = password,
    account = account
)

con.cursor().execute("USE WAREHOUSE COMPUTE_WH;") 

query_inf = "SELECT * FROM DEMO.INFORMATION_SCHEMA.TABLES;"

# con.execute =  "select distinct user_name from snowflake.account_usage.access_history;"
# query_inf = "SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));"

df = pd.read_sql_query(query_inf, con)
print(df)