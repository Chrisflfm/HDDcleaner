class DoubleFile:
      
    def __init__(self, id, fullName, dirname, basename, st_mode, st_mtime, st_size, st_atime, hash, onDisk):
        self.id = id
        self.fullName = fullName
        self.dirname = dirname
        self.basename = basename
        self.st_mode = st_mode
        self.st_mtime = st_mtime
        self.st_size = st_size
        self.ist_atimed = st_atime
        self.hash = hash
        self.onDisk = onDisk
        self.rootFolder = dirname[:dirname.find("\\", 4)]
        self.rootdepth = fullName.count("\\")


class RootFolder:
      
    def __init__(self, category_id, rootFolders, ChangeOk):
        self.category_id = category_id
        self.rootFolders = rootFolders
        self.ChangeOk = ChangeOk


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

def UpdateFile(id):
    con = getConnection()
    if con.is_connected():
        cursor = con.cursor()
        sql = "UPDATE cd_test.tblfile SET onDisk = '0' WHERE id = " + str(id)
        cursor.execute(sql)
        con.commit()
        if cursor.rowcount >0:
            return True

def GetRawDoubles():
    con = getConnection()
    if con.is_connected():
        cursor = con.cursor()
        sql_select_Query = (
            "SELECT a.* FROM cd_test.tblfile a JOIN (SELECT hash, COUNT(*) FROM cd_test.tblfile GROUP BY hash HAVING count(*) > 1 ) b ON a.hash = b.hash ORDER BY a.hash")
        cursor = con.cursor()
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()
        RawDoubles = dict()
        for row in records:
            dbl = DoubleFile(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
            if dbl.hash in RawDoubles.keys(): 
                RawDoubles[dbl.hash].append(dbl)
            else:
                dblst = list()
                RawDoubles[dbl.hash] = dblst
                RawDoubles[dbl.hash].append(dbl)
        
        return RawDoubles

def GetRootFolders():
    con = getConnection()
    if con.is_connected():
        cursor = con.cursor()
        sql_select_Query = (
            "SELECT * FROM cd_test.rootfolders")
        cursor = con.cursor()
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()
        rootFolders = dict()
        for row in records:
            rf = RootFolder(row[0], row[1], row[2])
            rootFolders[rf.rootFolders[0:-1]] = rf
        return rootFolders

# ##############################################################
# Initializing                                                 #
# ##############################################################

os.system('cls' if os.name == 'nt' else 'clear')

# report("*******************************************************", False)
print("Cleaner V0.1 started, getting raw data")
rawDoubles = GetRawDoubles()
print("Get root folder list")
rootFolders = GetRootFolders()
print("Processing")
FileCounter = 0
Bytes = 0
for key in rawDoubles:
    highRootCount = 0
    for x in rawDoubles[key]:
        if x.rootdepth > highRootCount:
            highRootCount = x.rootdepth
    for x in rawDoubles[key]:
        if x.rootdepth < highRootCount:
            FileCounter = FileCounter + 1
            Bytes = Bytes + x.st_size
            if rootFolders[x.rootFolder].ChangeOk == 1:
                if os.path.exists(x.fullName):
                    try:
                        os.remove(x.fullName) 
                    except:
                        pass
                UpdateFile(x.id)
print("Processing ended")
print(str(FileCounter) + " removed")
print(str(Bytes) + " bytes removed")


