import mysql.connector
import csv
import os

# Variables for IMDB connection
# https://relational.fit.cvut.cz/dataset/IMDb
imdb_server = 'relational.fit.cvut.cz'
imdb_port = '3306'
imdb_user = 'guest'
imbd_pass = 'relational'
imdb_database = 'imdb_ijs'

# Queries used to get DDL and data
sql_create_table = 'SHOW CREATE TABLE {table}'
sql_show_tables = 'SHOW TABLES'
sql_select = 'SELECT * FROM {table}'
tables = []

# Get the working directory to output information retrieved from DB
cwd = os.getcwd() + '/data/'

os.mkdir(os.path.dirname(cwd))


# Create connection and cursor to the IMDB database
cx = mysql.connector.connect(user=imdb_user, password=imbd_pass,
                              host=imdb_server,
                              database=imdb_database)
curs = cx.cursor()

print('Getting tables')
# Get tables in database
curs.execute(sql_show_tables)
data = curs.fetchall()
for row in data:
    tables.append(row[0])

print('Getting DDL')
# For every table in the database, get the create table statement and store the ouput to DDL
with open(cwd + '/DDL.sql', 'w+') as outfile:
    for table in tables:
        print('\t'+table)
        curs.execute(sql_create_table.format(table=table))
        data = curs.fetchall()
        for row in data:
            outfile.write(row[1] + ';\n')

print('Getting Data')
# Time to get the data
for table in tables:
    curs.execute(sql_select.format(table=table))
    print('\t' + table)
    with open(cwd + '/' + table + '.csv', 'w+') as outfile:
        writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
        prev_data = []

        while True:
            data = curs.fetchmany(10000)

            if data == prev_data or not data:
                break
            prev_data = data
            writer.writerows(data)

cx.close()


