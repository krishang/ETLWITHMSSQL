
# Author : KrishanG
# Date   : 24/05/2023
# Remarks: This is a helper class which creates SQL scripts to manupilate Microsoft SQL server databse objacts. Note this class is specific to MSSQL and I have not tested this 
# with either Postgres or MySQL
# download the missing packages using pip note the parameters to get pass the ssl error. this is if you are using somthing like netbox blue, which requires a local certificate.
# pip install --trusted-host=pypi.org --trusted-host=files.pythonhosted.org --user pymssql


import pandas as pd

class Helper(object):
 
    # only send only the first few rows of a dataset for this. e.g. ds.head(2) dont want to slow the function down.
    def clean_header( header):
        # Cleans the given header by removing special characters and making it suitable for storing in a database.
        # Remove non-alphanumeric characters and replace them with underscores
       # header = header.rename(columns=lambda x: x.replace('#', '_').replace('@', '_').replace('#', '_')
        #                       .replace('-', '').replace(' ', '_').replace('/','').replace('(','').replace(')','').replace(':','').replace('%','') 
                               
         #                      )
    
        for i in range(len(header)):
            header[i] = header[i].replace("*", "")
            header[i] = header[i].replace("$", "")
            header[i] = header[i].replace("#", "")
            header[i] = header[i].replace("/", "")

        return header

    # Define a function to clean column names
    def clean_column_names(df):
        df.columns = df.columns.str.lower()  # convert column names to lowercase
        df.columns = df.columns.str.replace('[^a-zA-Z0-9]+', '_', regex=True)  # replace non-alphanumeric characters with underscore
        df.columns = df.columns.str.strip('_')  # strip leading and trailing underscores
        return df


    def create_sql_table(df: pd.DataFrame, table_name: str) -> str:
        columns = []
        
        for col in df.columns:
            if df[col].dtype == 'object':
                columns.append(f'[{col}] NVARCHAR(4000)')
            elif df[col].dtype == 'datetime64[ns]':
                columns.append(f'[{col}] DATETIME')
            else:
                columns.append(f'[{col}] NVARCHAR(4000)')
        columns_str = ', '.join(columns)
        sql_str = f'CREATE TABLE [{table_name}] ({columns_str});'
        return sql_str


    def drop_sql_table(table_name:str)->str:
        sql_str=f"drop table if exists {table_name}; "
        return sql_str
        
    
    # Pre conditions make sure to have cleaned the dataframe header by removing any special characters before using this function. Use clean_header(self, header) for this

    def create_sql_insert_statements(dataframe, table_name):
        columns = ",".join([str(i) for i in dataframe.columns.tolist()])
        statements = []

        for _, row in dataframe.iterrows():
            values = tuple(str(val) if not pd.isna(val) else "" for val in row)
            statement = "INSERT INTO {} ({}) VALUES ({})".format(
                table_name, columns, ','.join(['%s'] * len(row))
            )
            statements.append((statement, values))

        return statements