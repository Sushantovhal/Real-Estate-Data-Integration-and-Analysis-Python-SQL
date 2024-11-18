import pandas as pd
import glob
import os

input_directory = 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/'
output_directory = 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Formatted/'

os.makedirs(output_directory, exist_ok=True)

date_columns = ['EffectiveDate__c', 'PublicationDate__c']

file_paths = glob.glob(os.path.join(input_directory, '*.csv'))


def process_file(file_path):
    df = pd.read_csv(file_path)

    if 'sequence_no' in df.columns:
        df.drop(columns=['sequence_no'], inplace=True)

    for column in date_columns:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors='coerce')
            df[column] = df[column].fillna(pd.to_datetime('1970/01/01'))
            df[column] = df[column].dt.strftime('%Y/%m/%d')

    output_file_path = os.path.join(output_directory, os.path.basename(file_path))

    df.to_csv(output_file_path, index=False)


for file_path in file_paths:
    process_file(file_path)

print("Date formatting, null date handling, and column deletion completed for all files.")
