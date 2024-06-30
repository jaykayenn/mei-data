import mysql.connector
import datetime

listfile = open('pidlist.txt', 'r')
data = listfile.read()
pidlist = data.split("\n")

mydb = mysql.connector.connect(
  host="192.168.1.135",
  user="jkn",
  password="JKn101274012",
  database="mei"
)

with open('Perlis_2013_Batch8.rpt', 'r', encoding='UTF-8') as file:
    wholeline = file.readline()
    cnt = 0
    while wholeline:
        pid = wholeline[:9].rstrip()
        #print(pid)
        if pid in pidlist:
            pid = int(wholeline[:9])
            cid = int(wholeline[12:15])
            #print(cid)
            actstr = wholeline[24:42]
            if "NULL" in actstr:
                actdate = datetime.date(1901, 1, 1)
                #print(actdate)
                acttime = datetime.time(0, 0, 0)
                #print(acttime)
            else:
                actdate = datetime.datetime.strptime(actstr, '%Y-%m-%d %H:%M:%S').date()
                #print(actdate)
                acttime = datetime.datetime.strptime(actstr, '%Y-%m-%d %H:%M:%S').time()
                #print(acttime)
            schstr = wholeline[48:67]
            if "NULL" in schstr:
                schdate = datetime.date(1901, 1, 1)
            else:
                schdate = datetime.datetime.strptime(schstr, '%Y-%m-%d %H:%M:%S').date()
                #print(schdate)
            #schtime = datetime.datetime.strptime(schstr, '%Y-%m-%d %H:%M:%S').time()
            #print(schtime)
            procname = wholeline[72:130].rstrip()
            #print(procname)
            resulttyp = wholeline[173:204].rstrip()
            #print(resulttyp)
            result = wholeline[204:]
            #print(result)
            mycursor = mydb.cursor()
            sql = "INSERT INTO perlis (patientid, clinicid, performedondate, performedontime, planneddate, labtestname, parameter, result) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            val = (pid, cid, actdate, acttime, schdate, procname, resulttyp, result)
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
            cnt += 1
            print(cnt)
        wholeline = file.readline()