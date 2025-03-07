import snowflake.connector
import json
import pandas as pd

with open('sfconfig.json','r') as file:
    data = json.load(file)
    user = data['user']
    password = data['password']
    account = data['account']
    warehouse = data['warehouse']

con = snowflake.connector.connect(
    user = user,
    password = password,
    account = account,
    warehouse = warehouse
)

query_inf = "SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS;"

df = pd.read_sql_query(query_inf, con)
print(df)