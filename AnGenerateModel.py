#!/usr/bin/python

#AU6:ET Sumt	ET Averg	E Mint Avrg Prev	TVK Sumt	TVK Averg	AZ:T Mint Avrg Prev			BC:Kapanış	Mint girerken lazım	Girerken Pişirme talimatı	Mint Girerken real	mint sonu kalan	ARA Üretim	BI:AraÜrtim sonrası kalan	ATIK	Toplam Atık	Toplam short	BM:ANOMALI


##	sqlite is an inmemory DB
##	import sqlite3
##	sqlfd = sqlite3.connect("intraProcess.DB")
##	cursor = sqlfd.cursor()
##	sql_command = """ SELECT * FROM t_Final; """
##	cursor.execute(sql_command)

#%matplotlib inline

import numpy as np
#import pandas as pd
#import scipy.stats as stats
#import matplotlib.pyplot as plt
#import sklearn

#from pandas import Categorical
import os
import time
import argparse
#time.sleep(2)
from subprocess import call

#from sklearn.dataset import load_boston
#boston=load_boston()

import copy
import pyodbc
#t_Final = []


def download_data(tblName, order):
	global targetWeek
	global FKstoreID
	global args
	global cursor
	sqlscript=("select *    FROM #%s %s" % (tblName, order))
	tblName = "%s-%s-%s" % (tblName, FKstoreID, targetWeek[:10])
	print ("--Prediction - Creating Model %s" % tblName)
	fd= open(tblName, 'w')
	if(args.Dbg == "d1" or args.Dbg == "d2"):
		print("--   download tbl into file %s" % tblName)
		print(sqlscript)
	cursor.execute(sqlscript)
	if (args.Dbg == "sql"):
		tTbl= cursor.fetchall()
		fd.writelines(["%s\n" % rowitem  for rowitem in tTbl])
	
	



def create_bkModel(mName, tMa, tMs, tMv):
	global targetWeek
	global FKstoreID
	global tActulaSales
	global BD72540
	global args
	global tModel
	mName = "%s-%s-%s" % (mName, FKstoreID, targetWeek[:10])
	if (args.Dbg == "d1"):
		print ("--Prediction - Creating Model %s" % mName)
	fd= open(mName, 'w')
	bkin= [BD72540,targetWeek,tModel[0][0],tModel[0][1],"IA:11:00","IK:23:00","6A:11:00","6K:24:00","7A:11:00","7K:23:30","H:PB","LM:ESGA18:00","DM:GSBJ18:00"]
	fd.write("%s\n" % (bkin))
	tcount= len(tModel)
	if (args.Dbg == "d1"):
		print("-- ---- %i" % tcount)
	ctr=162
	tcount=884+ctr+5
	while (ctr<tcount):
		fd.write("%s, %s, %s, %s\n" % (tModel[ctr], tMa[{ctr], tMs[ctr], tMv[ctr]))
		#if(ctr%20==0):
		#	print("%i:%s" % (ctr, tModel[ctr]))
		ctr= ctr+1
	fd.close()
	#call("touch Model.mod", shell=True) 


def create_actualSales():
	global targetWeek
	global FKstoreID
	global tActulaSales
	global cursor
	global args
	cursor.execute("INSERT INTO t_Final "+
	"SELECT Distinct FKStoreId, M.Hour, M.Mint, 0,0,0,0         ,0,0, M.DayOfWeek, DateOB "+
	"FROM #Sumt S, t_Mints M    WHERE S.DayOfWeek = M.DayOfWeek "+
	"and DateOB > DATEADD(day,-9,CAST('%s' as datetime))" % targetWeek		#really only need last week
	)
	cursor.commit()

	sqlstring= ("delete from t_actualSales where FKStoreID = %i") % (FKstoreID)
	cursor.execute(sqlstring)
	#cursor.execute("Create table t_actualSales (FKStoreId int, Hour int , Mint int, QnttyE int, QnttyT int, DayOfWeek int, DateOB datetime)")	
	sqlstring= ("insert into t_actualSales "+
	"SELECT FKStoreId, Hour, Minute/5, Sum(QnttyE), Sum(QnttyT), DATEPART(dw, A.DateOB), DateOB "+
	"FROM All_Sales_10_Restaurants A, #MenuKamp MK "+
	"WHERE  A.FKItemId = MK.FKItemId AND A.FKStoreId IN(%i) and DateOB between "+
	"CAST('%s' as datetime)  and  DATEADD(week,+1,CAST('%s' as datetime))    GROUP BY FKStoreId, DateOB, DATEPART(dw, A.DateOB), HOUR, Minute/5") % (FKstoreID, targetWeek, targetWeek)
	cursor.execute(sqlstring)
	cursor.commit()
	sqlstring= ("INSERT INTO t_actualSales "+
	"SELECT Distinct %i, M.Hour, M.Mint*2, 0, 0, S.DayOfWeek, S.DateOfBusiness "+
	"FROM  t_Mints M  left outer join t_actualSales S ON   S.DayOfWeek = M.DayOfWeek "+
	"and M.hour = S.hour and M.mint*2 = S.Mint   WHERE S.FKStoreID = %i") % (785, FKstoreID) #FKStoreID, FKStoreID)
	if (args.Dbg == "d1"):
		print(sqlstring)
	cursor.execute(sqlstring)
	sqlstring= ("INSERT INTO t_actualSales "+
	"SELECT Distinct %i, M.Hour, M.Mint*2+1, 0, 0, S.DayOfWeek, S.DateOfBusiness "+
	"FROM  t_Mints M  left outer join t_actualSales S ON   S.DayOfWeek = M.DayOfWeek "+
	"and M.hour = S.hour and M.mint*2+1 = S.Mint   WHERE s.FKStoreID = %i")  % (785, FKstoreID) #FKStoreID, FKStoreID)
	cursor.execute(sqlstring)
	cursor.commit()
	cursor.execute("select * from t_actualSales")
	tactSales= cursor.fetchall()
	cursor.commit()
	if (args.Dbg == "d1"):
		print ("--size of array tActualSales C:%i x R:%i " % (len(tactSales[0]), len(tactSales)))


def learn_bkdata(bkhr, sName, dbName, uName):
	global tModel
	global targetWeek
	global tActulaSales
	global FKstoreID
	global BD72540
	global args
	global cursor
	if (args.Dbg == "d1"):
		print ("--Prediction - Loading Transactions ")
	ParentList=(10001,10004,10005,10006,10018,10023,10030) #10007,10012
	bkstartHr= int(bkhr[1])
	bkstartMn= int(bkhr[2])
	bkstopHr= int(bkhr[3])
	bkstopMn= int(bkhr[4])
	bkmidnight= bool(bkstopHr<9)
	cnxn= pyodbc.connect('DRIVER={ODBC Driver 11 for SQL Server};SERVER='+sName+';DATABASE='+dbName+';UID='+uName+';PORT=1433;PWD=P@ssw0rd')	#//
	cursor= cnxn.cursor()
	#Joining Menus and Campaign ItemsID s
	cursor.execute("IF OBJECT_ID('tempdb..#MenuKamp') IS NOT NULL   DROP TABLE #MenuKamp")
	if args.Dbg == "sql":
		print("Create table     #MenuKamp (FKItemId int, ParentID int, QnttyE int, QnttyT int, Categ int);")	
	cursor.execute("Create table     #MenuKamp (FKItemId int, ParentID int, QnttyE int, QnttyT int, Categ int)")	
	if args.Dbg == "sql":
		print("insert into #MenuKamp "+
	"SELECT FKItemId, ParentCode,   Sum(QnttyE) as QnttyE, Sum(QnttyT) as QnttyT, 9 "+
	"FROM t_Kampanya K   GROUP BY FKItemId, ParentCode "
	"UNION SELECT FKItemId, ParentID,   Sum(QnttyE) as QnttyE, Sum(QnttyT) as QnttyT, 9 "+
	"FROM t_Menu WHERE FKItemID NOT IN (select FKItemId from t_kampanya) "+
	"GROUP BY FKITEMID, PARENTID ;")
	cursor.execute("insert into #MenuKamp "+
	"SELECT FKItemId, ParentCode,   Sum(QnttyE) as QnttyE, Sum(QnttyT) as QnttyT, 9 "+
	"FROM t_Kampanya K   GROUP BY FKItemId, ParentCode "
	"UNION SELECT FKItemId, ParentID,   Sum(QnttyE) as QnttyE, Sum(QnttyT) as QnttyT, 9 "+
	"FROM t_Menu WHERE FKItemID NOT IN (select FKItemId from t_kampanya) "+
	"GROUP BY FKITEMID, PARENTID ")
	cursor.commit()
	cursor.execute("select count(*) from #MenuKamp")
	tcount= int((cursor.fetchone())[0])
	if (args.Dbg == "d1"):
		if (tcount != 382):
			print("-- there seems to be a problem: Are there new Menu offerings %i" % tcount)
	#download_data("MenuKamp", " ORDER BY FKItemId, ParentID")
	
	cursor.execute("IF OBJECT_ID('tempdb..#Sumt') IS NOT NULL   DROP TABLE #Sumt")
	if args.Dbg == "sql":
		print     ("Create table     #Dates (DateOB datetime, Hour int, Mint int);")	
	cursor.execute("Create table     #Dates (DateOB datetime, Hour int, Mint int)")
	sqlstring= ("insert into #Dates    SELECT distinct DateOfBusiness, M.Hour, M.Mint "+
	"FROM All_Sales_10_Restaurants, t_Mints M "+
	"WHERE DateOfBusiness between DATEADD(week,-14,CAST('%s' as datetime)) and DATEADD(day, 8,CAST('%s' as datetime))" % (targetWeek, targetWeek))
	if args.Dbg == "sql":
		print(sqlstring)
	cursor.execute(sqlstring)
	cursor.commit()
	cursor.execute("select count(*) from #Dates")
	tcount= int((cursor.fetchone())[0])
	if (args.Dbg == "d1"):
		if (tcount != 13842):
			print("--There seems to be a problem [min 15 weeks]107*126:")
		print("--  Dates records %i(==13842)" % tcount)
	#download_data("Dates", " ORDER BY DateOB")
	
	cursor.execute("IF OBJECT_ID('tempdb..#Items') IS NOT NULL   DROP TABLE #Items")
	if args.Dbg == "sql":
		print("Create table     #Items (FKStoreId int, ParentID int, Qntty int, dist int) ;")	
	cursor.execute("Create table     #Items (FKStoreId int, ParentID int, Qntty int, dist int)")
	sqlstring= ("insert into #Items "+
	"SELECT FKStoreId, parentID, Sum(QnttyE)+Sum(QnttyT), 0 "+
	"FROM All_Sales_10_Restaurants A, #MenuKamp MK "+
	"WHERE  A.FKItemId = MK.FKItemId AND A.FKStoreId IN(%i) and DateOfBusiness IN (select distinct DateOB from #Dates) "+
	"GROUP BY FKStoreId, ParentID") % (FKstoreID)
	if args.Dbg == "sql":
		print(sqlstring)
	cursor.execute(sqlstring)
	cursor.commit()
	cursor.execute("select sum(qntty) from #Items")
	tcount= int((cursor.fetchone())[0])
	cursor.execute("update #Items set dist=qntty/%i " % tcount)
	cursor.execute("delete from #Items where dist < 3 ")
	cursor.execute("select count(*) from #Items")
	tcount= int((cursor.fetchone())[0])
	if (args.Dbg == "d1"):
		if (tcount < 9):
			print("--There seems to be a problem :")
		print("--  Items missing records %i(==9)" % tcount)
	#download_data("Items", " ORDER BY parentId")
	
	cursor.execute("IF OBJECT_ID('tempdb..#Sumt') IS NOT NULL   DROP TABLE #Sumt")
	if args.Dbg == "sql":
		print("Create table     #Sumt (FKStoreId int, Hour int , Mint int, QnttyE int, QnttyT int, DateOB datetime, DayOW int);")	
	cursor.execute("Create table     #Sumt (FKStoreId int, Hour int , Mint int, QnttyE int, QnttyT int, DateOB datetime, DayOW int)")
	sqlstring= ("insert into #Sumt "+
	"SELECT FKStoreId, Hour, Minute/10, Sum(QnttyE), Sum(QnttyT), DateOfBusiness, DATEPART(dw, A.dateOfBusiness) "+
	"FROM All_Sales_10_Restaurants A, #MenuKamp MK "+
	"WHERE  A.FKItemId = MK.FKItemId AND A.FKStoreId IN(%i) and DateOfBusiness IN (select distinct DateOB from #Dates) "+
	"GROUP BY FKStoreId, DateOfBusiness, HOUR, Minute/10") % (FKstoreID)
	if args.Dbg == "sql":
		print(sqlstring)
	cursor.execute(sqlstring)
	sqlstring= ("insert into #Sumt "+
	"SELECT %s, Hour, Mint, 0, 0, DateOB   FROM #Dates D "+
	"WHERE NOT EXISTS (SELECT * FROM #Sumt S WHERE S.Mint=D.Mint and S.Hour=D.Hour and S.DateOB=D.DateOB)") % (FKstoreID)
	if args.Dbg == "sql":
		print(sqlstring)
	cursor.execute(sqlstring)
	cursor.commit()
	cursor.execute("select count(*) from #Sumt")
	tcount= int((cursor.fetchone())[0])
	if (args.Dbg == "d1"):
		if (tcount < 6917 or tcount > 13482):	#7437:6917:7984
			print("--There seems to be a problem :")
		print("--  Mint records %i(==13482)" % tcount)
	#download_data("Sumt", " ORDER BY FKStoreId, DayOW, Hour, Mint, DateOB")
	
	cursor.execute("IF OBJECT_ID('tempdb..#Waverg') IS NOT NULL   DROP TABLE #Waverg")
	if args.Dbg == "sql":
		print("Create table     #Waverg (FKStoreId int, Hour int , Mint int, AvergE int, AvergT int, "+
	"QnttyE int, QnttyT int, DayOW int ,DateOB datetime);")	
	cursor.execute("Create table     #Waverg (FKStoreId int, Hour int , Mint int, AvergE float, AvergT float, "+
	"QnttyE int, QnttyT int, DayOW int ,DateOB datetime)")	
	if args.Dbg == "sql":
		print("insert into #Waverg "+
	"SELECT A.FKStoreId, Hour, Mint, "+
	"ROUND(AVG(QnttyE) OVER  (PARTITION BY FKStoreId , dayOW , HOUR , Mint "+
	"	ORDER BY  FKStoreId, HOUR, Mint, DateOB   ASC ROWS 13 PRECEDING),1), "+
	"ROUND(AVG(QnttyT) OVER  (PARTITION BY FKStoreId , dayOW , HOUR , Mint "+
	"	ORDER BY  FKStoreId, HOUR, Mint, DateOB   ASC ROWS 13 PRECEDING),1), "+
	"QnttyE, QnttyT, dayOW, DateOB    FROM #Sumt  A;")
	cursor.execute("insert into #Waverg "+
	"SELECT A.FKStoreId, Hour, Mint, "+
	"ROUND(AVG(QnttyE*10) OVER  (PARTITION BY FKStoreId , dayOW , HOUR , Mint "+
	"	ORDER BY  FKStoreId, HOUR, Mint, DateOB   ASC ROWS 13 PRECEDING),1), "+
	"ROUND(AVG(QnttyT*10) OVER  (PARTITION BY FKStoreId , dayOW , HOUR , Mint "+
	"	ORDER BY  FKStoreId, HOUR, Mint, DateOB   ASC ROWS 13 PRECEDING),1), "+
	"QnttyE, QnttyT, dayOW, DateOB    FROM #Sumt  A")
	cursor.commit()
	cursor.execute("select count(*) from #Waverg")
	tcount= int((cursor.fetchone())[0])
	if (args.Dbg == "d1"):
		if (tcount < 6917 or tcount > 13482):	#7437:6917:7984
			print("--There seems to be a problem :")
		print("--  menu types %s(==13482)" % tcount)
	#download_data("Waverg", " ORDER BY FKStoreId, DayOW, Hour, Mint, DateOB")
	
	#CREATE TABLE t_Final  (FKStoreId int, Hour int,  Mint int, AvergE int,  AvergT int, QE, QT, fctrE int, fctrT int, DayOW int,  DateOB datetime);
	sqlstring= ("delete  from t_Final  WHERE FKStoreID = %i") % FKstoreID
	if args.Dbg == "sql":
		print(sqlstring)
	cursor.execute(sqlstring)
	cursor.commit()
	if args.Dbg == "sql":
		print("INSERT INTO t_Final "+
	"SELECT FKStoreId, Hour, Mint, 0,0, QnttyE, QnttyT   ,0,0,  DayOW, DateOB "+
	"FROM #Sumt S    WHERE DateOB > DATEADD(day,-9,CAST('%s' as datetime))" % targetWeek )	
	cursor.execute("INSERT INTO t_Final "+
	"SELECT FKStoreId, Hour, Mint, 0,0, QnttyE, QnttyT   ,0,0,  DayOW, DateOB "+
	"FROM #Sumt S    WHERE DateOB > DATEADD(day,-9,CAST('%s' as datetime));" % targetWeek )
	cursor.commit()
	if args.Dbg == "sql":
		print("Update  t_Final "+
	"SET AvergE = A.AvergE, AvergT = A.AvergT,  QnttyE = A.QnttyE, QnttyT = A.QnttyT  "+
	"FROM #Waverg A     WHERE t_Final.FKStoreId = A.FKStoreId "+
	"AND t_Final.dateOB = A.DateOB AND t_Final.Mint = A.Mint AND t_Final.Hour = A.Hour;")
	cursor.execute("Update  t_Final "+
	"SET AvergE = A.AvergE, AvergT = A.AvergT,  QnttyE = A.QnttyE, QnttyT = A.QnttyT  "+
	"FROM #Waverg A     WHERE t_Final.FKStoreId = A.FKStoreId "+
	"AND t_Final.dateOB = A.DateOB AND t_Final.Mint = A.Mint AND t_Final.Hour = A.Hour")
	cursor.commit()
	sqlstring= ("select count(*) from t_Final WHERE FKStoreID = %i") % FKstoreID
	if args.Dbg == "sql":
		print(sqlstring)
	cursor.execute(sqlstring)
	tcount= int((cursor.fetchone())[0])
	if (args.Dbg == "d1"):
		if (tcount != 1134 and tcount != 2142):
			print("-- There seems to be a problem with  tFinal sum %s(==2142)" % tcount)
	sqlstring= (" from t_Final WHERE FKStoreID = %i  and DateOB between DATEADD(day,-8,CAST('%s' as datetime)) and DATEADD(day, 8,CAST('%s' as datetime)) ") % (FKstoreID, targetWeek, targetWeek)
	cursor.execute("select count(*) " +sqlstring)
	tcount= int((cursor.fetchone())[0])
	if (args.Dbg == "d1"):
		if (tcount != 1134 and tcount != 2142):
			print("-- There seems to be a problem with tFinal week %s(==2142)" % tcount)    #2188
		#return
	cursor.execute("select FKStoreId, Hour, Mint, AvergE, AvergT,     0, 0,  0, 0, DayOW, DATEADD(day,7,DateOB) " 
	+sqlstring+ " ORDER BY FKStoreId, DateOB, Hour, Mint")
	tModel= cursor.fetchall()
	blanks= [785, 23, 5, 0, 0            ,0,0,0,0,  2,2001]
	#print("--size of bl array C:%i x R:int " %  len(blanks))
	modelEnd = len(tModel) -1
	ctr= 0
	while (ctr<46):
		blanks= list(tModel[ctr+modelEnd])
		tModel.append(copy.deepcopy(blanks))
		ctr= ctr+1
	if (args.Dbg == "d1"):
		print("--size of array tModel C:%i x R:%i " % (len(tModel[0]), len(tModel)))
	tMa= np.average(tModel, axis=1)
	tMs= np.std(tModel, axis=1)#[, dtype, out, ddof, keepdims])
	tMv= np.var(tModel, axis=1)#[, dtype, out, ddof, keepdims])
	#BKregister_X = tModel.data[:, np.newaxis, 2]
	#BKregister_X_train = BKregister_X[:-20]
	#BKregister_X_test = BKregister_X[-20:]
	#BKregister_y_train = tModel.target[:-20]
	#BKregister_y_test = tModel.target[-20:]
	#regr.fit(BKregister_X_train, BKregister_y_train)
	# Make predictions using the data set
	#BKregister_y_pred = regr.predict(BKregister_X_test)
	ctr=160				#161
	tcount= 884 +ctr 	#1044
	while (ctr<tcount):
		ctr= ctr+1
		if(ctr > 3 and ctr < 1044):
			# opening and closing
			f1= BD72540
			if(tModel[ctr][1] < bkstartHr):
				f1 /= 80.0
			if(tModel[ctr][1] == bkstartHr and tModel[ctr][2] < bkstartMn):
				f1 /= 80.0
			if(bkmidnight):		#midnight is usually less predictable
				if(tModel[ctr][1] > bkstopHr and tModel[ctr][1] < 9 ):
					f1 /= 30.0
				if(tModel[ctr][1] == bkstopHr and tModel[ctr][1] < 9  and tModel[ctr][2] > bkstartMn):
					f1 /= 30.0
			else:
				if(tModel[ctr][1] > bkstopHr):
					f1 /= 20.0
				if(tModel[ctr][1] == bkstopHr and tModel[ctr][2] > bkstartMn):
					f1 /= 20.0
			f1=round(f1,  4)
			#gen the denominator for Model denom is of averages
			tModel[ctr][5]= (tModel[ctr-2][3]+tModel[ctr-1][3]+tModel[ctr][3])*2+f1
			tModel[ctr][6]= (tModel[ctr-2][4]+tModel[ctr-1][4]+tModel[ctr][4])*2+f1
		if(ctr>=tcount-6 or ctr<165):
			if (args.Dbg == "d2"):
				print("%i:%s" % (ctr, tModel[ctr]))
	cursor.commit()
#	create_actualSales()
	cursor.close()
	create_bkModel('bkModel', tMa, tMs, tMv)


global args
def load_bkParams(modifier, mName):
	global targetWeek
	global FKstoreID
	global BD72540
	global args
	print ("--Model Generation - Start ")
	parser = argparse.ArgumentParser(description='Learn from previous dataset')
	parser.add_argument('-Dbg', type=str, default='d0',	help='set debug level  d1 d2 etc')
	args = parser.parse_args()
	fd = open(mName, 'r')
	bkin=fd.readline().split('	')
	if (args.Dbg == "d1"):
		print(bkin)
	BD72540=float(bkin[0])
	#FKstoreID= int(bkin[1])
	targetWeek= bkin[2]
	ctr= 13
	while (ctr>0):
		ctr= ctr-1
		bkin=fd.readline().split('	')
		if args.Dbg == "d2":
			print (bkin)
	if (args.Dbg == "d1"):
		print ("--Prediction - Model Loaded ")
	fd = open("bkHours", 'r')
	if (args.Dbg == "d1"):
		print("week %s bd %f" % (targetWeek, BD72540))
	while (True):
		bkhr=fd.readline().split('	')
		if (args.Dbg == "d1"):
			print(bkhr)
		tcount= len(bkhr)
		if(tcount<2):
			break
		FKstoreID= int(bkhr[0])
		learn_bkdata(bkhr, '172.16.208.72', 'BK_HISTORICAL_ATP', 'crmadmin')
	call("od -x bkModel-* >  Model.mod", shell=True) 


modifier= 1.0
ctr= 0
while (ctr<4):
	modifier= load_bkParams(modifier, 'bkParams')
	ctr += 1