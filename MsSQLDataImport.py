# Author : KrishanG
# Date   : 24/05/2023
# Remarks: Get the config details when initializing the class. Remember to specify this in the config.ini file.
#########################################################################################################################


import os
import sys
import configparser
from datetime import datetime
import re

import pymssql
import petl
import pandas as pd

import MsSQLDataHelper as D

class DataImporter:
    def __init__(self, path=None, config_file='config.ini'):
        self.path = path
        self.config_file = config_file
        self.config = configparser.ConfigParser()
        self.start_date = None
        self.url = None
        self.dest_server = None
        self.dest_database = None
        self.c_user = None
        self.c_password = None
        self.csv_files = []
        self.tbl_errors = [['Log_date', 'ErrorLine']]

    def read_config(self):
        try:
            self.config.read(self.config_file)
            self.start_date = self.config['CONFIG']['startDate']
            self.url = self.config['CONFIG']['url']
            self.dest_server = self.config['CONFIG']['server']
            self.dest_database = self.config['CONFIG']['database']
            self.c_user = self.config['CONFIG']['User']
            self.c_password = self.config['CONFIG']['Password']
            self.path = self.config['CONFIG']['Path']
        except Exception as e:
            print('Could not read configuration file:', str(e))
            sys.exit()

    def import_data(self):
        self.read_config()
        self.csv_files = [f for f in os.listdir(self.path) if f.endswith('.csv') and not f.startswith('ETL_Err')]
    
        for file in self.csv_files:
            file_path = os.path.join(self.path, file)
            try:
                data = pd.read_csv(file_path, encoding='latin-1')
                data = D.Helper.clean_column_names(data)
            
                table_name = re.sub(r'[#\$%/\(\)]', '', os.path.splitext(file)[0])
                db_connection = pymssql.connect(server=self.dest_server, user=self.c_user, password=self.c_password, database=self.dest_database)
            
                sql = f"SELECT  count(*) as TableAvailable FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_NAME='{table_name}'" 
                is_table = petl.io.fromdb(db_connection, sql)['TableAvailable'][0]
             
                if is_table == 1:
                    sql = D.Helper.drop_sql_table(table_name)
                else:
                    sql = ''
            
                sql += D.Helper.create_sql_table(data.head(), table_name)
            
                lst_records = D.Helper.create_sql_insert_statements(data, table_name)
                sql_statements = lst_records
            
                try:
                    with db_connection.cursor() as cursor:
                        cursor.execute("BEGIN TRANSACTION")
                        cursor.execute(sql)
                    
                        for statement, values in sql_statements:
                            cursor.execute(statement, values)
                    
                        cursor.execute('COMMIT TRANSACTION')
                        db_connection.commit()
                except Exception:
                    db_connection.rollback()
                    raise
                finally:
                    cursor.close()
            
            except Exception as e:
                error_message = f'Could not open CSV: {file_path}. Detail error: {str(e)}'
                print(error_message)
                self.tbl_errors.append([datetime.now(), error_message])
