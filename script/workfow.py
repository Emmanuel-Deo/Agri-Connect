import pandas as pd
import json

import warnings
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

gisoutput = "../ProcessedData_2024/Helvetas/SUMS/output/Helvetas-Jul-Sep-2021-gis2.csv"
jsonoutput ="../ProcessedData_2024/Helvetas/SUMS/output/Helvetas-Jul-Sep-2021-json2.csv"

df = pd.read_csv("../ProcessedData_2024/Helvetas/SUMS/inputs/Helvetas-Jul-Sep-2021.csv")

df= df.drop('Code', axis = 1)

df = df.drop(0)

# Initialize the header variable
current_header = None

# Create a dictionary to store the mapping of headers
header_mapping = {}

for col in df.columns:
    if col.startswith("Unnamed:"):
        if current_header is not None:
            header_mapping[col] = current_header
    else:
        current_header = col

# Rename the DataFrame columns using the mapping
df = df.rename(columns=header_mapping)

# Define the new column names
new_column_names = {
    'Unnamed: 0': 'Region',
    'Unnamed: 1': 'District'
}

# Rename the specified columns
df = df.rename(columns=new_column_names)

# Extract the values from row 1
suffixes = df.iloc[0].tolist()

# Replace spaces with underscores in each feature
modified_list = [feature.replace(' ', '_') for feature in suffixes]

# Rename the columns with the values from row 1 as suffixes
df.columns = [f'{col}_{suffix}' for col, suffix in zip(df.columns, modified_list)]

df = df.drop(1)

# Define the new column names
new_column_names = {
    'Region_Region': 'Region',
    'District_District': 'District'
}

# Rename the specified columns
df = df.rename(columns=new_column_names)

df = df.reset_index(drop=True)

# Replace "Indicator" with 0 in a specific column (replace 'ColumnName' with the actual column name)
column_name = '3.4_Youth_Male'  # Replace with the actual column name

df[column_name] = df[column_name].replace('Jul - Sep 2021', 0)

# List of columns to exclude from conversion to int
exclude_columns = ["Region", "District"]

# Iterate through columns and convert to int if not in the exclusion list
for column in df.columns:
    if column not in exclude_columns:
        df[column] = pd.to_numeric(df[column], errors='coerce', downcast='integer')
        
df.fillna(0, inplace=True)

df.to_csv(gisoutput, index= False)


# Initialize an empty dictionary to store the transformed data
data = {}

# Iterate over each row in the DataFrame
for index, row in df.iterrows():
    district = row['District']
    data[district] = row.drop('District').to_dict()


# List of columns to exclude from conversion to int
exclude_columns = ["Region", "District"]

# Iterate through columns and convert to int if not in the exclusion list
for column in df.columns:
    if column not in exclude_columns:
        df[column] = pd.to_numeric(df[column], errors='coerce', downcast='integer')


import pandas as pd

# Assuming df is your DataFrame
# Create a nested dictionary
district_dict = {}
for index, row in df.iterrows():
    district_name = row["District"]
    district_dict[district_name] = {}

    for column in df.columns:
        if column != "District" and "_" in column:
            main_key, sub_key = column.split("_", 1)
            if main_key not in district_dict[district_name]:
                district_dict[district_name][main_key] = {}
            district_dict[district_name][main_key][column] = int(row[column])

# Convert to JSON
district_dict_json = json.dumps(district_dict, indent=2)

# Create a DataFrame
df_list = []
for district, values in district_dict.items():
    row = {"District": district}
    row.update(values)
    df_list.append(row)

df = pd.DataFrame(df_list)


# Convert all columns except 'District' to JSON in each cell
df_json = df.drop('District', axis=1).applymap(json.dumps)

# Add the 'District' column back to the DataFrame at the beginning
df_json = pd.concat([df['District'], df_json], axis=1)

df_json.columns = df_json.columns.str.replace('.', '_')


# Assuming df is your DataFrame
df_json.columns = ['code' + col.replace('_', '') if col != 'District' else col for col in df_json.columns]


import ast
df = df_json

# Convert string representations of dictionaries to actual dictionaries
for column in df.columns[1:]:
    df[column] = df[column].apply(ast.literal_eval)

# Remove prefixes from keys in nested dictionaries
for column in df.columns[1:]:
    df[column] = df[column].apply(lambda x: {key.split('_', 1)[1]: value for key, value in x.items()})


# Convert all columns except 'District' to JSON in each cell
df_json = df.drop('District', axis=1).applymap(json.dumps)

# Add the 'District' column back to the DataFrame at the beginning
df_json = pd.concat([df['District'], df_json], axis=1)


df_json = df_json.rename(columns=lambda x: x.replace(' ', ''))



# Assuming df is your DataFrame
# Replace df with your actual DataFrame variable

# Specify the current column names and the new column names
column_mapping = {
    'old_column_name1': 'code 1515',
    'old_column_name2': 'code1515',
    # Add more entries as needed
}

# Use the rename method to rename the columns
df_json = df_json.rename(columns=column_mapping)


# Define the new column names
new_column_names = {
    'District': 'district'
    
}

# Rename the specified columns
df_json = df_json.rename(columns=new_column_names)

df_json.to_csv(jsonoutput, index=False)

