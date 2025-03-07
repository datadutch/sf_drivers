import requests
from bs4 import BeautifulSoup
import pandas as pd
import snowflake.connector
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Function to fetch and create the first dataframe
def fetch_web_data():
    # Fetch the webpage content
    url = "https://docs.snowflake.com/en/release-notes/requirements"
    response = requests.get(url)
    response.raise_for_status()  # Raise an error if the request fails

    # Parse the webpage
    soup = BeautifulSoup(response.content, "html.parser")

    # Locate the table
    table = soup.find("table")  # Adjust if the specific table has attributes like id or class

    # Extract table headers
    headers = [th.text.strip() for th in table.find("thead").find_all("th")]

    # Extract table rows
    rows = []
    for tr in table.find("tbody").find_all("tr"):
        row = [td.text.strip() for td in tr.find_all("td")]
        # Handle rows with missing or extra columns
        while len(row) < len(headers):  # Fill missing columns with None
            row.append(None)
        rows.append(row[:len(headers)])  # Ensure row matches header count

    # Create Pandas dataframe
    df_web = pd.DataFrame(rows, columns=headers)

    # Fill empty "Type" cells with values from above (forward fill)
    if 'Type' in df_web.columns:  # Ensure the 'Type' column exists
        df_web['Type'] = df_web['Type'].replace('', pd.NA).fillna(method='ffill')

    # Remove rows where 'Recommended Version' is 'N/A'
    df_web = df_web[df_web['Recommended Version'] != 'N/A']

    # Create the join key
    df_web['join_key'] = df_web['Type'].str.split().str[0]

    return df_web

# Function to execute Snowflake query and return dataframe
def execute_snowflake_query(query):
    with open('sfconfig.json', 'r') as file:
        data = json.load(file)
        user = data['user']
        password = data['password']
        account = data['account']
        warehouse = data['warehouse']

    con = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse
    )
    return pd.read_sql_query(query, con)

# Function to fetch sessions data
def fetch_snowflake_sessions_data():
    query_inf = """
    SELECT USER_NAME, CLIENT_APPLICATION_ID
    FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS;
    """
    return execute_snowflake_query(query_inf)

# Function to fetch users data
def fetch_snowflake_users_data():
    query_users = """
    SELECT NAME AS USER_NAME, EMAIL
    FROM SNOWFLAKE.ACCOUNT_USAGE.USERS;
    """
    return execute_snowflake_query(query_users)

# Function to send email
def send_email(to_address, subject, body):
    from_address = 'your_email@example.com'  # Replace with your email address
    password = 'your_email_password'         # Replace with your email password

    msg = MIMEMultipart()
    msg['From'] = from_address
    msg['To'] = to_address
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP('smtp.example.com', 587)  # Replace with your SMTP server address and port
        server.starttls()
        server.login(from_address, password)
        text = msg.as_string()
        server.sendmail(from_address, to_address, text)
        server.quit()
        print(f'Email sent to {to_address}')
    except Exception as e:
        print(f'Failed to send email to {to_address}: {e}')

# Fetch data and create dataframes
df_web = fetch_web_data()
df_snowflake_sessions = fetch_snowflake_sessions_data()
df_snowflake_users = fetch_snowflake_users_data()

# Remove duplicates based on CLIENT_APPLICATION_ID
df_snowflake_sessions = df_snowflake_sessions.drop_duplicates(subset=['CLIENT_APPLICATION_ID'])

# Print the content of the CLIENT_APPLICATION_ID column for debugging
print("Snowflake Data CLIENT_APPLICATION_ID (Full Set):")
print(df_snowflake_sessions['CLIENT_APPLICATION_ID'])

# Create the join key for the snowflake sessions dataframe
df_snowflake_sessions['join_key'] = df_snowflake_sessions['CLIENT_APPLICATION_ID'].str.split().str[0]

# Function to extract the version part safely
def extract_version(client_app_id):
    # Check if the value is None or does not contain a valid version
    if pd.isna(client_app_id):
        return 'No version'
    try:
        # Split from the right and get the last part
        version = client_app_id.rsplit(' ', 1)[-1]
        # Check if the extracted part is a valid version (contains digits and dots)
        if any(char.isdigit() for char in version):
            return version
        return 'No version'
    except Exception:
        return 'No version'

# Apply the function to extract the version
df_snowflake_sessions['version'] = df_snowflake_sessions['CLIENT_APPLICATION_ID'].apply(extract_version)

# Print the key columns and join keys for debugging
print("Web Data Join Keys:")
print(df_web[['Type', 'Recommended Version', 'join_key']])

print("\nSnowflake Data Join Keys:")
print(df_snowflake_sessions[['CLIENT_APPLICATION_ID', 'USER_NAME', 'join_key', 'version']])

# Join the sessions dataframe with the web dataframe on the join keys
merged_df = pd.merge(df_web, df_snowflake_sessions, left_on='join_key', right_on='join_key', how="inner")

# Filter out rows where the version and Recommended Version are the same
filtered_df = merged_df[merged_df['version'] != merged_df['Recommended Version']]

# Join the filtered dataframe with the users dataframe on USER_NAME
final_df = pd.merge(filtered_df, df_snowflake_users, left_on='USER_NAME', right_on='USER_NAME', how="left")

# Print the full merged dataframe
print("\nFinal Merged DataFrame:")
with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    print(final_df[['join_key', 'CLIENT_APPLICATION_ID', 'USER_NAME', 'EMAIL', 'version', 'Recommended Version']])

# Send emails to addresses in the final dataframe
for index, row in final_df.iterrows():
    if pd.notna(row['EMAIL']):
        subject = f"Update Required: Upgrade to Recommended Version {row['Recommended Version']}"
        body = f"Dear {row['USER_NAME']},\n\nYour current version ({row['version']}) is different from the recommended version ({row['Recommended Version']}). Please upgrade to ensure compatibility.\n\nBest regards,\nYour Team"
        send_email(row['EMAIL'], subject, body)