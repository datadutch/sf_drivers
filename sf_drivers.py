import requests
from bs4 import BeautifulSoup
import pandas as pd

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

# Print headers and first few rows for debugging
print("Headers:", headers)
print("First few rows:", rows[:5])

# Create Pandas dataframe
df = pd.DataFrame(rows, columns=headers)

# Fill empty "Type" cells with values from above (forward fill)
if 'Type' in df.columns:  # Ensure the 'Type' column exists
    df['Type'] = df['Type'].replace('', pd.NA).fillna(method='ffill')

# Print the dataframe to verify
print(df)
