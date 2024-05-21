import os
import csv
import traceback

import mysql.connector
from tabulate import tabulate

try:
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        password='ciao',
        database='dcrb_out'
    )

    if mydb.is_connected():
        cursor = mydb.cursor()
        print('Connessione al database riuscita')
        search_target: str = input('Input the word you want to search\n')
        search_query = """select t2.id, t2.count, files_info.name, files_info.type, files_info.size, files_info.location, 'in_content' as result_location
from (select id, count
	from (select id,
	round((length(trim(html)) - length(replace(trim(html), '{}','')))/length('{}')) as count
from files_content ) as t1
where t1.count != 0) as t2 join files_info on t2.id = files_info.id

union all

select id, 1 as count, name, type, size, location, 'in_name' as result_location 
from files_info
use index (path)
where path like binary '%{}%'
 
 """.format(search_target, search_target, search_target)
        cursor.execute(search_query)
        result = cursor.fetchall()
        print(tabulate(result, headers=['Id', 'Count','Name', 'Type', 'Size', 'File_location', 'Match_location'], tablefmt='orgtbl'))
except e:
    print('Errore nella connessione al database')
