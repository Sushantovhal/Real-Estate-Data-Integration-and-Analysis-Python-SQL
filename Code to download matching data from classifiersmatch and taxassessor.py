import mysql.connector
import pandas as pd
import os
import tkinter as tk
from tkinter import filedialog


def get_folder_path():
    root = tk.Tk()
    root.withdraw()
    folder_path = filedialog.askdirectory()
    if not folder_path:
        raise ValueError("No folder selected.")
    return folder_path


folder_path = get_folder_path()

batch_size = 50000

connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='rootadmin@115',
    database='buildtax1'
)

try:
    file_index = 1
    last_sequence_no = 0
    total_records_processed = 0

    while True:
        query = f"""
            SELECT * FROM taxassessor ta
            JOIN classifiersmatch cm
            ON ta.Attom_Id__c = cm.Attom_Id__c1
            WHERE cm.sequence_no > {last_sequence_no}
            ORDER BY cm.sequence_no
            LIMIT {batch_size} 
        """
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()
        if not result:
            break

        df = pd.DataFrame(result)

        start_seq = total_records_processed + 1
        end_seq = total_records_processed + len(df)
        file_name = os.path.join(folder_path, f'Matched_From_Taxassessor_and_Classifiersmatch({start_seq} to {end_seq}).csv')
        df.to_csv(file_name, index=False)

        print(f'{file_name} has been created with {len(df)} rows.')

        total_records_processed += len(df)
        file_index += 1
        last_sequence_no = df['sequence_no'].iloc[-1]

    cursor.close()

finally:
    connection.close()
