# Import __packages
import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql.functions import  to_date
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql.functions import isnan, col

# unmount from path
dbutils.fs.unmount("/mnt/gdp")

# mount
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "",
"fs.azure.account.oauth2.client.secret": '',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/tanent_id/oauth2/token"}


dbutils.fs.mount(
source = "abfss://container-name@stotage-name.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolymic",
extra_configs = configs)

# check folder list
%fs
ls "/mnt/gdp"

# Read data before reading i granted an access to read and write the storaage
df = spark.read.format("csv").option("header", "true").load("/mnt/gdp/raw-gdp-data/")

# check the imported data
df.show()

# check the column and datatype
df.printSchema()

# filter by year 
# Assuming date_column is the name of the date column in your DataFrame
start_date = '2010'
end_date = '2022'

# Convert the 'Year' column to datetime type if it's not already
# df['Year'] = pd.to_datetime(df['Year'])

# Filter the DataFrame based on the condition
filtered_df = df[(df['Year'] >= start_date) & (df['Year'] <= end_date)]

# dron nan value 
gdp_cleaned_data = filtered_df.dropna()

# write data back to storage
gdp_cleaned_data.write.mode("overwrite").option("header", "true").csv("/mnt/gdp/transform-data/gdp_cleaned_data")