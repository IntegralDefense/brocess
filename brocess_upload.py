#!/usr/bin/env python3
#

import argparse
import configparser
import pymysql
import sqlite3

parser = argparse.ArgumentParser(description="Transfer local sqlite3 database to mysql database.")
parser.add_argument('sqlitedb', help="Path to the sqlite database to load.")
parser.add_argument('-i', help="Path to configuration file. Defaults to brocess.ini")
args = parser.parse_args()

source_connection = sqlite3.connect(args.sqlitedb)
source_cursor = source_connection.cursor()

config = configparser.ConfigParser()
config.read('brocess.ini' if not args.i else args.i)

dest_connection = pymysql.connect(host=config['mysqli']['server'], user=config['mysqli']['username'],
                                  password=config['mysqli']['password'], db=config['mysqli']['database'])
dest_cursor = dest_connection.cursor()
counter = 0
limit = 1000
total = 0
source_cursor.execute("SELECT host, numconnections, firstconnectdate FROM httplog")
for host, numconnections, firstconnectdate in source_cursor:
    dest_cursor.execute("""insert into httplog (host,numconnections,firstconnectdate) values (%s,%s,%s)
                           on duplicate key update numconnections = numconnections + %s""", (host, numconnections, firstconnectdate, numconnections))
    counter += 1
    total += 1
    if counter >= limit:
        dest_connection.commit()
        counter = 0

dest_connection.commit()
dest_connection.close()

print("transfered {} entries".format(total))
