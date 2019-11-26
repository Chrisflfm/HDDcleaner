import glob
import os
from datetime import datetime
import hashlib
import sys
import traceback
import mysql.connector
from mysql.connector import Error

def errorHandler(e):
    try:
        txt = traceback.format_exc()
        traceback.print_exc(file=sys.stdout)
        print(txt)
    except Exception as ex:
        print("errorHandler fout: ", ex)


def errorNoHandler():
    return


def getConnection():
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='cd_test',
                                             user='root',
                                             password='Murbella*')

        if connection.is_connected():
            return connection
        else:
            print("No connection!")
            errorHandler()

    except Error as e:
        print("Error while connecting to MySQL", e)


def closeConnection(connection):
    try:
        if (connection.is_connected()):
            cursor = connection.cursor()
            cursor.close()
            connection.close()
            # print("MySQL connection is closed")
    except Exception as e:
        errorHandler(e)


def GetRawDoubles():
    con = getConnection()
    if con.is_connected():
        cursor = con.cursor()
        sql_select_Query = (
            "SELECT  a.* FROM cd_test.tblfile a JOIN (SELECT hash, COUNT(*) FROM cd_test.tblfile GROUP BY hash HAVING count(*) > 1 ) b ON a.hash = b.hash ORDER BY a.hash")
        cursor = con.cursor()
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()
        RawDoubles = list()
        for row in records:
            dbl = DoubleFile(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8])
            RawDoubles.append(dbl)
        return RawDoubles

# ##############################################################
# Initializing                                                 #
# ##############################################################

os.system('cls' if os.name == 'nt' else 'clear')

# report("*******************************************************", False)
print("Cleaner V0.1 started")
rawDoubles = GetRawDoubles()
print("Processing ended")


class DoubleFile:
    def __init__(self, id, fullName, dirname, basename, st_mode, st_mtime, st_size, st_atime, hash):
        self.id = id
        self.fullName = fullName
        self.dirname = dirname
        self.basename = basename
        self.st_mode = st_mode
        self.st_mtime = st_mtime
        self.st_size = st_size
        self.ist_atimed = st_atime
        self.hash = hash
