#!/usr/bin/python3

import os
import csv
import datetime
import re
import mysql.connector

def connect_db(host, user, pwd, db, auth):
    db_conn = mysql.connector.connect(
        host=host, 
        user=user, 
        password=pwd, 
        database=db, 
        auth_plugin=auth
    )
    return db_conn




def create_tables(db_connection, maxsize):
    db_reference = db_connection.cursor()
    db_reference.execute("SET FOREIGN_KEY_CHECKS = 0")
    db_reference.execute("DROP TABLE IF EXISTS files_info")
    db_connection.commit()
    db_reference.execute("""
        CREATE TABLE files_info (
            id INT NOT NULL AUTO_INCREMENT, 
            name VARCHAR(255) NOT NULL, 
            path VARCHAR(255),
            type VARCHAR(255),
            size BIGINT,
            location VARCHAR(512), 
            PRIMARY KEY(id)
        )
    """)
    db_reference.execute("DROP TABLE IF EXISTS files_content")
    db_connection.commit()
    db_reference.execute("""
        CREATE TABLE files_content (
            `id` INT NOT NULL AUTO_INCREMENT,
            `html` MEDIUMTEXT NULL,
            PRIMARY KEY (`id`))
            ENGINE = InnoDB
            DEFAULT CHARACTER SET = utf8mb4
            COLLATE = utf8mb4_0900_ai_ci;""")
    db_reference.execute("SET NAMES 'UTF8MB4'")
    db_reference.execute("SET CHARACTER SET UTF8MB4")
    db_connection.commit()


def insert_info_tuple(db_connection, info):
    cursor = db_connection.cursor()
    info_insert_query = """
        INSERT INTO files_info (id, name, path, type, size, location) 
        VALUES (%s, %s, %s, %s, %s,%s)
        """
    cursor.executemany(info_insert_query, info)
    db_connection.commit()


def insert_into_file_content(db_connection, info):
    cursor = db_connection.cursor()
    data_insert_query = """
    INSERT INTO files_content (id, html)
    VALUES (%s, %s)
    """
    cursor.executemany(data_insert_query, info)
    db_connection.commit()


def create_index(db_connection):
    db_reference = db_connection.cursor()
    db_reference.execute("CREATE FULLTEXT INDEX contend_index ON files_content (html);")
    db_reference.execute("CREATE INDEX name_index ON files_info (name);")
    db_connection.commit()


def remove_char_from_string(string):
    for c in "#?$&/%!\"£*,:;_-#":
        string = string.replace(c, " ")
    return string



def check_and_submit(batch_info, batch_data, MAX_BATCH_SIZE, db_connection):
    if len(batch_info) == MAX_BATCH_SIZE:
        print(batch_info)
        insert_info_tuple(db_connection, batch_info)
        insert_into_file_content(db_connection, batch_data)
        print("debug returnign empty list")
        return [], []
    else:
        return batch_info, batch_data



def populate_db(startpath, db_connection, MAX_BATCH_SIZE):
    batch_info = []
    batch_data = []
    counter = 1
    db_reference = db_connection.cursor()
    print("Populating started...")
    for root, dirs, files in os.walk(startpath):
        print("Walking...")
        for file in files:
            print("File"+ str(file))
            file_path = os.path.join(root, file)
            file_size = os.path.getsize(file_path)
            name, extension = os.path.splitext(file_path)
            name = os.path.basename(file_path)
            print(file_size, file, extension, root)
            print(len(batch_info))
            if (len(batch_info) < MAX_BATCH_SIZE):
                try:
                   # print("DEBUG-TRY-1")
                    if extension == ".html":
                        with open(file_path, 'r', encoding='utf-8') as file:
                            print("file open")
                            txt = str(file.read())
                            batch_info.append([counter, name, file_path, extension, file_size, root])
                            batch_data.append([counter, txt])
                            counter += 1
                    else:
                        batch_info.append((counter, name, file_path,  extension, file_size, root))
                    counter += 1
                except Exception as e:
                    print("ERRORE"+ str(e))
                    continue
            else:
                print("MAX BATCH SIZE REACHED")
                batch_info, batch_data = check_and_submit(batch_info, batch_data, MAX_BATCH_SIZE, db_connection)
            batch_info, batch_data = check_and_submit(batch_info, batch_data, MAX_BATCH_SIZE, db_connection)

        for dir in dirs:
            if len(batch_info) < MAX_BATCH_SIZE:
                try:
                    dir_path = os.path.join(root, dir)
                    dir_size = os.path.getsize(dir_path)
                    batch_info.append([counter,dir, dir_path, "directory", dir_size, root])
                    counter += 1
                except Exception as e:
                    counter += 1
                    continue
            else:
                batch_info, batch_data = check_and_submit(batch_info, batch_data, MAX_BATCH_SIZE, db_connection)
            batch_info, batch_data = check_and_submit(batch_info, batch_data, MAX_BATCH_SIZE, db_connection)


    insert_info_tuple(db_connection, batch_info)
    insert_into_file_content(db_connection,batch_data)
    db_reference.close()




db_connection = connect_db("localhost", "root", "ciao", "dcrb_out", "mysql_native_password")
create_tables(db_connection, 255)
MAX_BATCH_SIZE = 10
populate_db('C:/Users/flavi/Desktop/DigitalContentMod2/', db_connection, MAX_BATCH_SIZE)
create_index(db_connection)
db_connection.close()

