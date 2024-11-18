input_file = 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/ENERGYAID_BUILDINGPERMITCLASSIFIERS_0001_002.txt'
output_file = 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cleaned_data.txt'

with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
    for line in infile:
        clean_line = ' '.join(line.strip().split())
        outfile.write(clean_line + '\n')

print("Data cleaned and written to", output_file)
