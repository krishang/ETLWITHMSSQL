# Initial commit: ETL script for importing CSV files into MSSQL database

This commit introduces the initial version of the ETL script that enables importing a collection of CSV files into a Microsoft SQL Server (MSSQL) database. The script utilizes the power of Pandas for data processing and facilitates the creation of corresponding tables in the database, with each table named after its respective CSV file.

To begin using this script, please edit the config.ini file and provide the necessary information. Specify the folder path containing the CSV files and the required connection details for your MSSQL server. It's important to ensure that the user defined in the configuration has DDL admin rights to create tables in your MSSQL database.

The script aims to simplify the process of importing CSV data into an MSSQL database by automating the table creation and data loading tasks. By leveraging the capabilities of Pandas.

Please note that additional steps, such as data validation and transformation, can be added to this script based on specific requirements.


