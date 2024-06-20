import os
import pandas as pd
import mysql.connector
 
# Directory containing the CSV files
csv_directory = r"C:\Users\Rajeh Swami\Documents\Aurora docs\update-data\data 2 (1)\data 2"
 
# MySQL connection details
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Amandeep@$123',
    'database': 'ihq'
}
 
# Establish a connection to the MySQL database
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    print("Connected to MySQL database")
except mysql.connector.Error as err:
    print(f"Error connecting to MySQL: {err}")
    exit(1)
 
# List all CSV files in the directory
csv_files = [f for f in os.listdir(csv_directory) if f.endswith('.csv')]
 
# Iterate over CSV files
for csv_file in csv_files:
    csv_path = os.path.join(csv_directory, csv_file)
    try:
        # Read CSV file
        df = pd.read_csv(csv_path, encoding='utf-8',sep=',',on_bad_lines='skip')
       
    except Exception as e:
        print(f"Error reading {csv_file}: {e}")
        continue
 
    # # Example: Print first few rows
    # print(f"First few rows of {csv_file}:\n{df.head()}")
 
    # Generate the table name from the CSV file name (remove the .csv extension)
    table_name = os.path.splitext(csv_file)[0]
    # print(f"Creating table: {table_name}")
 
    # Handle None values in column names and truncate to 64 characters
    columns = df.columns.tolist()
    truncated_columns = {col: col[:64] if col else 'None' for col in columns}
 
    # Generate the CREATE TABLE statement
    column_definitions = []
    for col in columns:
        col_name = col.replace('`', '``')  # Escape backticks in column names
        truncated_col = truncated_columns[col].replace('`', '``')  # Escape backticks in truncated column names
        if df[col].dtype == object:
            max_length = df[col].str.len().max()
            if max_length > 65535:
                col_type = "TEXT"  # or "MEDIUMTEXT" depending on your data
            elif max_length > 255:
                col_type = "VARCHAR(255)"
            else:
                col_type = f"VARCHAR({max_length})"
        elif pd.api.types.is_integer_dtype(df[col]):
            col_type = "BIGINT"
        elif pd.api.types.is_float_dtype(df[col]):
            col_type = "FLOAT"
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            col_type = "DATETIME"
        else:
            col_type = "VARCHAR(255)"
 
        column_definitions.append(f"`{truncated_col}` {col_type}")
 
    create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(column_definitions)})"
    # print(f"Create table query for {table_name}:\n{create_table_query}")
    try:
        cursor.execute(create_table_query)
        # print(f"Table {table_name} created successfully")
    except mysql.connector.Error as err:
        print(f"Error creating table {table_name}: {err}")
        continue
 
    # Insert data into the table
    for i, row in df.iterrows():
        row = [None if pd.isna(val) else val for val in row]
 
        for idx, val in enumerate(row):
            if isinstance(val, str) and len(val) > 255:
                row[idx] = val[:255]
 
        placeholders = ', '.join(['%s'] * len(row))
        insert_query = f"INSERT INTO `{table_name}` ({', '.join([f'`{truncated_columns[col]}`' for col in columns if col in truncated_columns])}) VALUES ({placeholders})"
        try:
            cursor.execute(insert_query, tuple(row))
        except mysql.connector.Error as err:
            print(f"Error inserting row into {table_name}: {err}")
            continue
 
# Commit the transactions and close the connection
conn.commit()
# print("Transaction committed")
 
# Close cursor and connection
cursor.close()
conn.close()
# print("Connection closed")