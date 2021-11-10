import os, sys, csv
import pandas as pd
import sqlite3
from datetime import datetime
import json


def getSettings(settingsFile="settings.json"):
    with open(settingsFile, "r") as f:
        settingsData = json.load(f)
        workingPath = settingsData['APP']['PATH']
        time4AggregatesInMins = settingsData['APP']['TIME4AGGREGATEINMINS']
        logFileName4Output = settingsData['APP']['LOGFILENAME']
        logFilePath = settingsData['APP']['LOGFILEPATH']
        errorLogFileName = settingsData['APP']['ERRORLOGFILENAME']
        dbFileName = settingsData['DATABASE']['DBFILENAME']
        dbTableName = settingsData['DATABASE']['DBNAME']
        apiUserName = settingsData['API']['USERNAME']
        apiPassword = settingsData['API']['PASSWORD']
        apiPasswordt = settingsData['API']['PASSWORDT']
        apiPasswordi = settingsData['API']['PASSWORDI']
        apiPasswordp = settingsData['API']['PASSWORDP']
        apiUrl = settingsData['API']['URL']
        apiUrlt = settingsData['API']['URLT']
        apiUrli = settingsData['API']['URLI']
        apiUrlp = settingsData['API']['URLP']
        settingsDict = {
            "workingPath": workingPath,
            "logFilePath": logFilePath,
            "errorLogFileName": errorLogFileName,
            "logFileName4Output": logFileName4Output,
            "time4AggregatesInMins": time4AggregatesInMins,
            "dbFileName": dbFileName,
            "dbTableName": dbTableName,
            "dbTableName": dbTableName,
            "dbTableName": dbTableName,
            "dbTableName": dbTableName,
            "apiUserName": apiUserName,
            "apiPassword": apiPassword,
            "apiPasswordt": apiPasswordt,
            "apiPasswordi": apiPasswordi,
            "apiPasswordp": apiPasswordp,
            "apiUrl": apiUrl,
            "apiUrlt": apiUrlt,
            "apiUrli": apiUrli,
            "apiUrlp": apiUrlp
            }
        return settingsDict

def getFlsOfType(lfPath,ext):
    #get all log-files
    logFils = []
    for file in os.listdir(lfPath):
        if file.endswith(".%s" %ext):
            logFils.append(os.path.join(lfPath, file))
    return logFils

def tableCheck(tabName,cursor):
    tabExists = False
    #get the count of tables with the name
    #sqlstmt = "SELECT name, count(name) FROM sqlite_master WHERE type='table' AND name='%s';" %tabName
    sqlstmt = "SELECT name FROM sqlite_master WHERE type='table' ;"
    cursor.execute(sqlstmt)
    #if the count is 1, then table exists
    for el in cursor.fetchall():
        if el[0] == tabName:
            tabExists = True
            break
    return tabExists

def connect2Db(dbName):
    # Connecting to the geeks database
    connection = sqlite3.connect(dbName)
    
    # Creating a cursor object to execute
    # SQL queries on a database table
    cursor = connection.cursor()
    
    return connection,cursor

def readInLogFlData(errorLogFile,logFiles, cursor, dbTableName, connection):
    with open("%s_%s.log" %(errorLogFile,datetime.today().strftime('%Y-%m-%d')),'a') as errOutputFl:
        for fl in logFiles:
            print("%s processing %s...\n" %(str(datetime.now()),fl))
            with open(fl) as csv_file:
                #csv_reader = DictReader(csv_file, delimiter=' ')
                csv_reader = csv.reader(csv_file, delimiter=' ')
                line_count = 0
                #colList = []
                #colList4Imp = []
                #col4ImpStr = ''
                for row in csv_reader:
                    valList = []
                    #if the proper line with all the columnnames is found in the file to read in
                    #create the string to prepare the new table
                    colDef = None
                    if row[0][0:7] == '#Fields':
                        colList = []
                        colList4Imp = []
                        col4ImpStr = ''

                        for col in row:
                            colList.append("%s TEXT " %col)
                            colList4Imp.append(col)
                        colDef = ', '.join(colList[1:])
                        colDef = colDef.replace("-","_").replace("(","").replace(")","")
                        if len(colList4Imp)==1:
                            col4ImpStr = ",".join(col.split("\t")[1:]) #+ ', cs_bytes'
                        else:
                            col4ImpStr = ", ".join(colList4Imp[1:])
                        col4ImpStr = col4ImpStr.replace("-","_").replace("(","").replace(")","")
                        if not tableCheck(dbTableName,cursor):
                            sqlstmt = """CREATE TABLE IF NOT EXISTS %s(id INTEGER PRIMARY KEY AUTOINCREMENT, servicename TEXT DEFAULT '-', %s);""" %(dbTableName,colDef)
                            try:
                                cursor.execute(sqlstmt)
                                connection.commit()
                            except:
                                sqlstmt = "delete from %s;" %dbTableName
                                cursor.execute(sqlstmt)
                                connection.commit()
                         
                    # if the read-in lines contain the values - prepare the SQL string to import them into 
                    # the table iisImp
                    elif row[0][0:1] != '#':
                        for val in row:
                            valList.append("'%s'" %val)

                        if len(row)==1:
                            valDef = ",".join("".join(row).split("\t")[1:])
                            valDef = "'" + valDef.replace(",","','") + "'" 
                            valDef = valDef.replace("\\","")
                            print(valDef)    
                        else:
                            valDef = ', '.join(valList)
                        # run the insert
                        sqlstmt = "Insert into %s(%s) VALUES(%s);" %(dbTableName,col4ImpStr,valDef)
                        #print(sqlstmt)
                        try: 
                            cursor.execute(sqlstmt)
                            connection.commit()
                        except:
                            print("*************ERROR*********")
                            print("ERROR with command '", sqlstmt, "'")
                            errOutputFl.write(sqlstmt+ "\n")
                            print("\n")
    connection.commit()
    print("*"*100)
    print("Logfile Import finished")

def updateServiceNameFromColumnValue(dbTableName,connection):
    with connection:
        curCur = connection.cursor()
        sqlstmt = "select cs_uri_stem from %s;" %dbTableName
        for  row in curCur.execute(sqlstmt):
            if len(row[0].split("/")) >= 5 and row[0].split("/")[1] == 'arcgis' and row[0].split("/")[2] == 'services':
                serviceSubStr = '/' + ('/'.join(row[0].split("/")[1:5]))
                val2Upd = row[0].split("/")[4]
                if len(val2Upd) < 3:
                    print(" --------- SHORT UpdateValue: ", val2Upd)
                sqlstmt2 = "update %s set servicename = '%s' where substr(cs_uri_stem,1,length('%s')) == '%s';" %(dbTableName,val2Upd,serviceSubStr,serviceSubStr)
                #print(sqlstmt)
                curCur.execute(sqlstmt2)
                connection.commit()
        connection.commit()
        print("Finished updateServiceNameFromColumnValue")


def updateServiceNameFromColumnValue1(dbTableName,connection):
    with connection:
        curCur = connection.cursor()
        #sqlstmt = "update {} set servicename = replace(cs_uri_query,'useExisting=1&layers=','') where {}.id in (select id from {} where cs_uri_query like 'useExisting=1&layers=%' );".format(dbTableName,dbTableName,dbTableName)
        sqlstmt = "update {} set servicename = substr(replace(cs_uri_stem,'/arcgis/services/',''),instr(replace(cs_uri_stem,'/arcgis/services/',''),'/')+1,instr(substr(replace(cs_uri_stem,'/arcgis/services/',''),instr(replace(cs_uri_stem,'/arcgis/services/',''),'/')+1,1000),'/')-1) where substr(cs_uri_stem,1,17) == '/arcgis/services/';".format(dbTableName)

        #print(sqlstmt)
        curCur.execute(sqlstmt)
        connection.commit()


def updateServiceNameFromColumnValue2(dbTableName,connection):
    with connection:
        curCur = connection.cursor()
        sqlstmt = "update {} set servicename = replace(cs_uri_query,'useExisting=1&layers=','') where {}.id in (select id from {} where cs_uri_query like 'useExisting=1&layers=%' );".format(dbTableName,dbTableName,dbTableName)
        #print(sqlstmt)
        curCur.execute(sqlstmt)
        connection.commit()


def updateServiceNameFromAPI(dbTableName,connection,url,usr,pwd):
    try: 
        from arcgis.gis import GIS
        gis = GIS(url,usr,pwd,verify_cert=False)
    except:
        gis = None 
    with connection:
        curCur = connection.cursor()
        whereCond = "csReferer like 'çlayers=ç'".replace("ç","%")
        sqlstmt = "select id, csReferer from %s where %s order by id;" %(dbTableName, whereCond)
        #cursor2 = connection.cursor()
        res = curCur.execute(sqlstmt)
        for row in res:
            serviceName = ''
            curId = row[0]
            #print(curId)
            curServiceId = row[1].split("layers=")[1]
            serviceName = getNameFromId(gis,curServiceId)
            sqlstmt2 = "update %s set servicename = '%s' where id == %i;" %(dbTableName,serviceName,curId)
            try:
                print("{} replaced by {}\n").format(curServiceId,serviceName)
            except:
                print("SOME ERROR IN THE API PROCESS\n %s\n" %sqlstmt2)
            curCur.execute(sqlstmt2)
        connection.commit()
    print("Finished updateServiceNameFromAPI\n")

def getNameFromId(gis,serviceId):
    serviceName = None
    try:
        return gis.content.get(serviceId).title
    except:
        return serviceId

def runQuery4Report(colNames4Report,queryString,cursor,expFlName,outputOption):        
    with open(expFlName,outputOption) as outputFl:
        outputFl.write("\n" + colNames4Report+"\n")
        for row in cursor.execute(queryString):
            outputFl.write(str(row).replace("(","").replace(")","")+"\n")

################################################################
def main():  
    settingsDict = getSettings()
    
    startTime = datetime.now()
    dbName = os.path.join(settingsDict['workingPath'],settingsDict['dbFileName'])
    logFilePath = settingsDict['logFilePath']
    logFileName4output = settingsDict['logFileName4Output']
    time4AggregatesInMins = settingsDict['time4AggregatesInMins']
    if os.path.exists(dbName):
        os.remove(dbName)
    errorLogFile = os.path.join(settingsDict['workingPath'], settingsDict['errorLogFileName'])
    dbTableName = settingsDict['dbTableName']
    url = settingsDict['apiUrl']
    usr = settingsDict['apiUserName']
    pwd = settingsDict['apiPassword']
    #'/Users/hansjoerg.stark/Library/Mobile Documents/com~apple~CloudDocs/Business/FUB/Projekte/IIS-Log Auswertung/iisLogs.db'
    # create db-connection
    print("connect to sqlite-database...\n")
    connection, cursor = connect2Db(dbName)
    
    ######Testing
    #updateServiceNameFromAPI(dbTableName,cursor,connection,url,usr,pwd)
    #sys.exit()
    # Analyse imported data

    #runQuery(sqlstmt, cursor, '_outputAnalysis_%s.txt' %datetime.today().strftime('%Y-%m-%d'), 'w')

    #sys.exit()

    # Drop Table to write in logfile contents if it exists
    sqlstmt = "Drop Table IF EXISTS %s;" %dbTableName
    cursor.execute(sqlstmt)
    connection.commit()

    # Read in all data from the logfiles and store them in the table iisImp in the SQLITE3 database
    print("Get list of logfiles...\n")
    logFiles = getFlsOfType(logFilePath,"log")

    # Read in all the logfile contents into the database
    print("Import logfiles...\n")
    readInLogFlData(errorLogFile,logFiles, cursor,dbTableName, connection)

    # Extract the service-name where it can be extracted
    print("Getting service-names from ArcGIS API...\n")
    updateServiceNameFromAPI(dbTableName, connection, url, usr, pwd)
    print("Updateing service-names from logfiles part I...\n")
    updateServiceNameFromColumnValue1(dbTableName,connection)
    print("Updateing service-names from logfiles part II...\n")
    updateServiceNameFromColumnValue2(dbTableName,connection)

    # Analyse imported data
    print("Analysing data...\n")
    sqlstmt = """
    select  count(*), cs_username, servicename, date, substr(time,1,5) as timestamp from %s 
        where servicename != '-' AND cs_username != '-'
        Group by cs_username, servicename, date, substr(time,1,5)
        order by date, timestamp asc;
    """ %dbTableName
    sqlstmt = """
    select  count(*), cs_username, servicename, date, substr(time,1,5) as timestamp from %s 
        where servicename != '-' AND cs_username != '-'
        Group by cs_username, servicename, date, substr(time,1,5)
        order by date, timestamp asc;
    """ %dbTableName
    timeGroupStmt = "round(strftime('çs', time)/{},0)".format(float(time4AggregatesInMins)*60).replace("ç","%") 
    sqlstmt = """
        select count(*), cs_username, servicename, date from 
        (
        select  cs_username, servicename, date, time from %s 
        where servicename != '-' AND cs_username != '-'
        Group by cs_username, servicename, date, %s
        order by date,time asc
        )
        group by 4,3,2
        order by 4,2,3;
    """ %(dbTableName,timeGroupStmt)
    outFl = '%s_%s.txt' %(logFileName4output,datetime.today().strftime('%Y-%m-%d'))
    outFlHhtp = '%s_httpReport_%s.txt' %(logFileName4output,datetime.today().strftime('%Y-%m-%d'))
    colNames4Report = 'Anzahl, Username, Servicename, Datum'
    runQuery4Report(colNames4Report, sqlstmt, cursor, outFl, 'w')

    # Analysis of http-Stati per user
    sqlstmt = "select count(*), cs_username, sc_status from %s group by 2,3 order by 3;" %dbTableName
    colNames4Report = 'Anzahl, Username, http-Status'
    runQuery4Report(colNames4Report, sqlstmt, cursor, outFlHhtp, 'w')
    print("Finishing up...\n")


    endTime = datetime.now()
    print("The process took %s secs" %str(endTime - startTime))
if __name__ == "__main__":
    main()

