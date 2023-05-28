# Author : KrishanG
# Date   : 24/05/2023
# Remarks: Use this script to import csv files from a given folder location and upload this into a MSSQL database. 
# The Prerquists are the folder path and also the database connection information needs to be defined in a config.ini file. 
# Note: The script generated is ONLY FOR MS SQL SERVER. If YOU NEED ANOTHER DB PLS UPDATE THE HELPER CLASS

import sys
from MsSQLDataImport import DataImporter

def main():
    config_file = 'config.ini'
    
    try:
        data_importer = DataImporter(config_file=config_file)
        data_importer.import_data()
        
    except FileNotFoundError:
        if len(sys.argv) < 4:
            print("Please provide the path, server, database, user, and password arguments.")
            print("Usage: python script.py <path> <server> <database> <user> <password>")
            return
        
        path = sys.argv[1]
        server = sys.argv[2]
        database = sys.argv[3]
        user = sys.argv[4]
        password = sys.argv[5]
        
        # Create the config file from the provided arguments
        config = f"[CONFIG]\nstartDate = \nurl = \nserver = {server}\ndatabase = {database}\nUser = {user}\nPassword = {password}\nPath = {path}\n"
        with open(config_file, 'w') as f:
            f.write(config)
        
        data_importer = DataImporter(path=path, config_file=config_file)
        data_importer.import_data()

if __name__ == '__main__':
    main()