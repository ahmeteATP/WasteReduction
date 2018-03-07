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

#from sklearn.dataset import load_boston
#boston=load_boston()

import copy
import pyodbc
tFinal = []
tModel = []


def create_bkResults(mName):
	global targetWeek
	global FKstoreID
	global tActulaSales
	global BD72540
	global args
	mName = "%s-%s-%s" % (mName, FKstoreID, targetWeek[:10])
	print ("Prediction - Creating Results %s" % mName)
	fd= open(mName, 'w')
	tcount= len(tFinal)
	if (args.Dbg == "d1"):
		print(" ---- %i" % tcount)
	ctr=36
	tcount=882+ctr
	while (ctr<tcount):
		fd.write("%s\n" % (tFinal[ctr]))
		#if(ctr%20==0):
		#	print("%i:%s" % (ctr, tModel[ctr]))
		ctr= ctr+1
	fd.close()

	

def learn_bkdata(sName, dbName, uName):
	global tModel
	global tFinal
	global targetWeek
	global FKstoreID
	global BD72540
	global args
	if (args.Dbg == "d1"):
		print ("Prediction - Loading Transactions ")
	tcount= len(tModel)	#890
	for col in [0, 5]:  #, 5, 10, 15, 20, 25, 30     #(10001,10004,10005,10006,10018,10023,10030) 
		tFinal[tcount][4]=0
		ctr= 4;
		tF3= 0
		tF4sum= 0
		tF5sum= 0
		tF6= 0		#give it an initial value
		tF7= 0		#give it an initial value
		tF8ctr= 0
		tF8sum= 0
		dt= int(col/5)
		if (args.Dbg == "d1"):
			print("\n\n\ndt=%i, col=%i\n\n\n" % (dt,col))
		while (ctr<882):  #+4<<-4
				ctr += 1
				ctf= ctr+33
				#Model[][9,10] not used
				#print("ctr %i ctf %i " %(ctr, ctf))
				tF2=0
				bde= float(tModel[ctr][5+dt].split(".")[0])/10
				fe1= float(tModel[ctr][5+dt])%1
				if(bde != 0):	
					tF2= round(min(((tFinal[ctf-1][col]+tFinal[ctf][col])* 3/ bde +0.9)/ 2, 1.1) *fe1*1.4,  4)
				#
				# Burdan sonrası Mint Sonu yani bu dömnem sonu real rakamlar gerekiyor !!!
				#
				# Yeni real harcamayı oku 5 dakikada bir
				#tF0= fd.read()
				# tamgereken5 + araPişen7 - harcanan0 => sonkalan9
				# gereken4 - sonkalan9 = tamgereken5
				# Öncesinde Lazım Hedef planlama-4 		BU HESAP İÇİN GERÇEKLEŞMİŞ OLAN VERİ LAZIM  !!!
				nF1= (float(tModel[ctr][3+dt])+float(tModel[ctr+1][3+dt])+float(tModel[ctr+2][3+dt])+float(tModel[ctr+3][3+dt])+float(tModel[ctr+4][3+dt]))/50
				nF2= (float(tModel[ctr-4][3+dt])+float(tModel[ctr-3][3+dt])+float(tModel[ctr-2][3+dt])+float(tModel[ctr-1][3+dt])+float(tModel[ctr][3+dt]))/50
				nF3= (float(tFinal[ctr-4][3+col])+float(tFinal[ctr-3][3+col])+float(tFinal[ctr-2][3+col])+float(tFinal[ctr-1][3+col])+float(tFinal[ctr][3+col]))/5
				nF4= nF2/(nF2+nF3)+0.5
				
				tF1= float(float(tModel[ctr][3+dt])/10+(tFinal[ctf][col])+(tFinal[ctf-1][col])) *fe1
				tF4= min(tF1, (float(tModel[ctr+1][3+dt])+float(tModel[ctr+2][3+dt])+float(tModel[ctr+3][3+dt]))/10)
				tF4sum += tF4
				#Mint Girerken PİŞİRME-5  =IF((B9/5--)<0;B4;IF((B4/2-B9/5--)<=0;0;B4-B9/5--))
				if(int(tFinal[ctf-1][col+4])>0):
					if (tF4-float(tFinal[ctf-1][col+4])>0):
						tF5= tF4-tFinal[ctf-1][col+4]
					else: tF5= 0
				else: tF5= tF4
				if(tF5>1):
					tF5 += 4- (tF5)% 4
				tF5 = round(tF5,0)
				tF5sum += tF5
				#Mint Girerken real ELDE-6	=IF((B7--)>0;B7--+B5/3;B5/3)
				#tF66= tF6
				if(tF7>0):				# this is the previous tF7
					tF6= tF5+ tF7
				else:
					tF6= tF5 
				tF6 = tF5 + tFinal[ctf-1][col+4]
				#mint sonu kalan7   
				# there is a problem with this number  !!!!!!!!!!
				tF7= tF6- int(tFinal[ctf+1][col])  # this is the  future QnttyE
				tF00=0	#tF00= fd.read()
				#ARA Üretim-8			=IF(B7<0;ROUND(B0++;0);0)
				#if(tF4*0.66-tF8<=0):	#arapişirme  needed
				tF8= 0		#give it an initial value
				if((tF6- int(tFinal[ctf+1][col])/2)<0):
					tF1= float(float(tModel[ctr+1][3+dt])/10+(tFinal[ctf+1][col])+(tFinal[ctf-0][col])) *fe1
					tF4= min(tF1, (float(tModel[ctr+1][3+dt])+float(tModel[ctr+2][3+dt])+float(tModel[ctr+3][3+dt]))/10)
					tF8= tF4 +4 -(tF4) %4
					tF8sum += tF8
					#tFinal[ctf][col+2]= tF5+ tF8
					tF8ctr += 1		#keep track of how many ara-pişirme is needed
					#tFinal[ctf][0]= int(tFinal[ctf][0])+ tF8
					#tFinal[ctf][5]= tF5- int(tFinal[ctf][0])
					tF7 += tF8
		
				#print("%i: %s" % (ctr, (tModel[ctr])))
				#tF4		#Öncesinde Lazım Hedef planlama-4
				tFinal[ctf][col+1]= tF5			#Mint Girerken PİŞİRME-5 
				tFinal[ctf][col+2]= tF6			#Mint Girerken real ELDE-6
				#tF7							#mint sonu kalan7   
				tFinal[ctf][col+3]= tF8			#ARA Üretim-8	
				tFinal[ctf][col+4]= max(tF7,0)#Final Remaining
				#if(tFinal[ctf][7]==14):
				#	print("%i:%i" % (ctr, ctf))
				#print("%s,  others: %i, %i, %i" % (tFinal[ctf], tF1, tF4, tF7 ))
	create_bkResults("results")
	if (args.Dbg == "d1"):
		print("\n\n,%i,%i,,, ,,,, ,,,, ,158 / %i, %i \n" % (tF4sum, tF5sum, tF8ctr, tF8sum))
	#fd.close()


def load_bkModel(mName, sName, dbName, uName):
	global tModel
	global tFinal
	global targetWeek
	global FKstoreID
	global args
	parser = argparse.ArgumentParser(description='Learn from previous dataset')
	parser.add_argument('-BK', type=str, default='ss',	help='run for BK restaurant storeID')
	parser.add_argument('-Dbg', type=str, default='d0',	help='set debug level  d1 d2 etc')
	args = parser.parse_args()
	fd = open("bkParams", 'r')
	bkin=fd.readline().split('	')
	BD72540=float(bkin[0])
	targetWeek= bkin[2]
	FKstoreID= int(args.BK)
	mName= "%s-%s-%s" % (mName, FKstoreID, targetWeek)
	if (args.Dbg == "d1"):
		print ("Prediction - Loading Model %s" % mName)
	fd = open(mName, 'r')
	blanks= [0, 0, 0, 0, 0, 0, 0,   0, 0]
	cnxn= pyodbc.connect('DRIVER={ODBC Driver 11 for SQL Server};SERVER='+sName+';DATABASE='+dbName+';UID='+uName+';PORT=1433;PWD=P@ssw0rd')
	cursor= cnxn.cursor()
	#the following read should really be from actualSales
	sqlstring= ("  from t_Final WHERE FKStoreID = %i  and DateOB between CAST('%s' as datetime)  and DATEADD(day,8,CAST('%s' as datetime))  " % (FKstoreID, targetWeek, targetWeek))
	#print("select count(*) " +sqlstring)
	cursor.execute("select count(*) " +sqlstring)
	tcount= int((cursor.fetchone())[0])
	if (tcount != 1134 and tcount != 2142):
		if (args.Dbg == "d1"):
			print(" There seems to be a problem:")
		print("tFinal %s" % tcount)
	#print         ("select  QnttyE,0,0,0,0,  QnttyT,0,0,0,0,   Hour, Mint, DateOB " +sqlstring+ " ORDER BY FKStoreId, DateOB, Hour, Mint")
	cursor.execute("select  QnttyE,0,0,0,0,  QnttyT,0,0,0,0,   Hour, Mint, DateOB " +sqlstring+ " ORDER BY FKStoreId, DateOB, Hour, Mint")
	tFinal= cursor.fetchall()
	if (args.Dbg == "d1"):
		print ("size of array tFinal C:%i x R:%i " % (len(tFinal[0]), len(tFinal)))
	cursor.close()
	ctr=0
	tcount= 882 +8
	while (ctr<tcount):  #197):
		line= fd.readline().strip().strip("('").strip(")']")
		#tModel.append(list(line.splitlines()))
		tModel.append(list(line.split(", ")))
#		tFinal.append(list(blanks))
		if (args.Dbg == "d1"):
			if(ctr>=tcount-6 or ctr<6):
				print("%i:%s" % (ctr, tModel[ctr]))
		ctr= ctr+1
	tcount= len(tModel)
	if (args.Dbg == "d1"):
		print ("size of array tModel C:%i x R:%i " % (len(tModel[0]), len(tModel)))
		print ("Prediction - Model Loaded ")
	if (tcount != 890 and tcount != 2142):   #158):  894
		if (args.Dbg == "d1"):
			print(" there seems to be a problem with tModel size %s" % ctr)
	#	return
	learn_bkdata('172.16.208.72', 'BK_HISTORICAL_ATP', 'crmadmin')


load_bkModel('bkModel', '172.16.208.72', 'BK_HISTORICAL_ATP', 'crmadmin')