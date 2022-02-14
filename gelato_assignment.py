import pandas as pd
import requests
import sys
from sqlalchemy import create_engine


pd.set_option('display.max_columns', 10)


if len(sys.argv) < 3:
    print("Running script with default arguments. You can also pass the url of the json datafile to be loaded and the  "
          "database table destination name.\n"
          "python.exe {} [url_to_json_file] [destination_table]\n".format(sys.argv[0]))

URL = 'https://data.cdc.gov/api/views/cjae-szjv/rows.json?accessType=DOWNLOAD' if len(sys.argv) < 3 else sys.argv[1]

print("Downloading dataset... {}".format(URL))
response = requests.get(URL)
data_json = response.json()
column_info = data_json['meta']['view']['columns']
data = data_json['data']

column_dict = {}
columns_to_remove = []
string_to_keep = "monitor and modeled"
table_dest = 'AIR_QUALITY_MEASURES' if len(sys.argv) < 3 else sys.argv[2]
query1 = "SELECT count(*) FROM sys.{}".format(table_dest)
query2 = "SELECT * FROM sys.{} LIMIT 5".format(table_dest)

# Load name and type for each column in json
for column in column_info:
    column_dict[column['name']] = column['dataTypeName']
    if column['id'] == -1:
        columns_to_remove.append(column['name'])

# Get data and put in a pandas df
df = pd.json_normalize(data_json, record_path=['data'])

print("Loaded {} registers.".format(len(df.index)))

# Renaming columns
df.columns = column_dict.keys()

# Convert column types
for k, v in column_dict.items():
    if v == 'number':
        df[k] = pd.to_numeric(df[k]).round(2)

# Remove columns not needed
print("Removing metadata columns... {}".format(columns_to_remove))
df.drop(columns_to_remove, axis=1, inplace=True)

# Also remove those rows that data is not completed with Downscaler (CDC and EPA predict model)
print("Discarding registers with incomplete data...")
df1 = df[df["MeasureName"].str.contains(string_to_keep)]

print("Discarded registers: ", len(df.index) - len(df1.index))
print("Number of registers being sinked into DB: {}".format(len(df1.index)))

# Connecting to db
print("Connecting to db...")
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                       .format(user="root",
                               pw="admin123",
                               host="localhost",
                               db="sys"))

print("Connected!\nLoading data into table...")
df1.to_sql(table_dest, con=engine, if_exists='replace', chunksize=10000)

print("Done.\n")

print("Executing queries:\n{}".format(query1))
result = engine.execute(query1)
result2 = engine.execute(query2)
for row in result:
    print(row)

print(query2)
for row in result2:
    print(row)
