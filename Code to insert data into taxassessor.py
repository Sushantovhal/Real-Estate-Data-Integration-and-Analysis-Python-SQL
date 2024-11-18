import pandas as pd
import mysql.connector
import numpy as np

site_df = pd.read_csv("2002.csv")

site_df.replace({np.nan: None}, inplace=True)

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'rootadmin@115',
    'database': 'buildtax1'
}

connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()

create_table_query = """
CREATE TABLE IF NOT EXISTS TaxAssessor (
    Attom_Id__c int PRIMARY KEY,
    FirstName varchar(700),
    MiddleName varchar(700),
    LastName varchar(700),
    Owner_Type__c varchar(200)
);
"""

cursor.execute(create_table_query)

insert_query = """INSERT IGNORE INTO TaxAssessor (
    Attom_Id__c, FirstName, MiddleName, LastName, Owner_Type__c
) VALUES (%s, %s, %s, %s, %s)"""

data_to_insert = [tuple(row) for row in site_df.itertuples(index=False, name=None)]

try:
    cursor.executemany(insert_query, data_to_insert)
    connection.commit()
except mysql.connector.Error as err:
    print(f"Error: {err}")

cursor.close()
connection.close()
