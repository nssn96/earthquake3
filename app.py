# Name : Surya Narayanan Nadhamuni Suresh
# UTA ID : 1001877873

from flask import Flask,render_template, request,url_for,flash
import mysql.connector as mysql
from geopy.geocoders import Nominatim as nm
from geopy.point import Point
from math import radians, sin, cos,sqrt,atan2
import decimal
import time


app = Flask(__name__)
app.secret_key = 'random string'

#DB connection details
HOST='utacloud1.reclaimhosting.com'
USER = 'sxn7873_surya'
PASSWORD='Pn2E)^Gq&Dc]'
DATABASE='sxn7873_adb'

#A specific function to set up a db connection
def dbConnect():
    global conn # defining a global variable
    #connect to database
    conn = mysql.connect(host=HOST,user=USER,password=PASSWORD,database=DATABASE)
    return conn

#Fuction to return the content of the whole table
def allData():
    dbConnect()
    cursor = conn.cursor()
    cursor.execute(mainQuery)
    res = cursor.fetchall()
    conn.close()
    return res

# the main select query
mainQuery = "Select * from earthquake "

def setProfile():
    print('Inside the setProfile function')
    setQuery = "SET  profiling = 1"
    dbConnect()
    cursor = conn.cursor()
    cursor.execute(setQuery)
    print("executed the set profile function")

def showProfiles():
    print('Inside the showProfiles function')
    query= "SHOW PROFILES"
    dbConnect()
    cursor = conn.cursor()
    cursor.execute(query)
    res = cursor.fetchall()
    print(res)

def startTime():
    start = time.time()
    return start

def endTime():
    end = time.time()
    return end


def largestN(fields):
    query=mainQuery
    dbConnect()
    cursor = conn.cursor()
    cursor.execute("SET  profiling = 1")
    for key,value in fields.items():
        query+="order by mag desc LIMIT 0,"+value
        #query+="where mag > "+value+" order by mag desc"
    print(query)
    
    cursor.execute(query)
    res = cursor.fetchall()
    return res

def dateRange(fields):
    query=mainQuery
    dbConnect()
    cursor = conn.cursor()
    query+=" where DATE(time)>='"+fields['From']+"' and Date(time)<='"+fields['To']+"' and mag>"+fields['Mag']
    print(query)
    cursor.execute(query)
    res = cursor.fetchall()
    conn.close()
    return res





@app.route('/')
def index():
    return render_template('index.html')

@app.route('/largest',methods=['GET','POST'])
def search():
    start = time.time()
    if request.method=='POST':
        dic={}
        for key,value in request.form.items():
            if value!='':
                dic[key]=value
        
        print(dic)

        if dic:
            result=largestN(dic)
        else:
            result=[]
            flash('Please enter values in the field')
    
    else:
        result = allData()

    end = time.time()
    print(end-start)
    return render_template('index.html', data=result,time=end-start)

@app.route('/date', methods=['GET','POST'])
def date():
    start = time.time()
    if request.method=='POST':
        dic={}
        result=[]
        for key,value in request.form.items():
            if value!='':
                dic[key] = value
        
        #print(dic)
        if dic:
            result = dateRange(dic)
            if result==[]:
                result==[]
                flash ('No Such entries in table')
        else:
            flash('Please enter values in fields')
        
    end = time.time()
    print(end-start)
        
    return render_template('index.html', data=result,time=end-start)



if __name__ == "__main__":
    app.run()

