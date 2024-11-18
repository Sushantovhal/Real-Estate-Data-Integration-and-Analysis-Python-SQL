import tkinter as tk
from tkinter import filedialog
import csv


def convert_txt_to_csv(txt_file_path, csv_file_path, delimiter=' '):
    with open(txt_file_path, 'r') as txt_file:
        lines = txt_file.readlines()

    with open(csv_file_path, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        for line in lines:
            row = line.strip().split(delimiter)
            writer.writerow(row)


def select_file():
    txt_file_path = filedialog.askopenfilename(
        title="Select a text file",
        filetypes=(("Text files", "*.txt"), ("All files", "*.*"))
    )
    if txt_file_path:
        csv_file_path = filedialog.asksaveasfilename(
            title="Save as",
            defaultextension=".csv",
            filetypes=(("CSV files", "*.csv"), ("All files", "*.*"))
        )
        if csv_file_path:
            convert_txt_to_csv(txt_file_path, csv_file_path)


root = tk.Tk()
root.withdraw()


select_file()


root.destroy()
