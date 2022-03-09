# Name : Surya Narayanan Nadhamuni Suresh
# UTA ID : 1001877873

from flask import Flask,render_template, request,url_for,flash
import mysql.connector as mysql
from geopy.geocoders import Nominatim as nm
from geopy.point import Point
from math import radians, sin, cos,sqrt,atan2
from decimal import Decimal
import time
import redis
import datetime
import ast


app = Flask(__name__)
app.secret_key = 'random string'

#DB connection details
HOST='utacloud1.reclaimhosting.com'
USER = 'sxn7873_surya'
PASSWORD='Pn2E)^Gq&Dc]'
DATABASE='sxn7873_adb'

#REDIS connection details
R_HOST='earthquake3.redis.cache.windows.net'
R_PASSWORD='ab41AFIzZ4MkAyBuzw8Qo4rxtc1iaCWnsAzCaLwQaVY='

r_cache = redis.StrictRedis(host=R_HOST,port=6380, db=0, password=R_PASSWORD, ssl=True)



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
        r_key=''
        for key,value in request.form.items():
            if value!='':
                dic[key]=value
                r_key+="_"+key+"_"+value
        
        print(dic)
        print(r_key)
        
        if dic:
            #cache hit--> then we can get from redis cache
            if r_cache.exists(r_key):
                print('Present in cache')
                result = eval(r_cache.get(r_key).decode("utf-8"))
            #cache miss--> if not in redis cache,then DB call and add new one to redis cache
            else:
                print('Not in cache')
                result=largestN(dic)
                r_cache.set(r_key,str(result))

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
        r_key=''
        result=[]
        for key,value in request.form.items():
            if value!='':
                dic[key] = value
                r_key+="_"+key+"_"+value
        
        #print(dic)
        print(r_key)
        if dic:
            #cache hit--> then we can get from redis cache
            if r_cache.exists(r_key):
                print('Present in cache')
                result = eval(r_cache.get(r_key).decode("utf-8"))
            #cache miss--> if not in redis cache,then DB call and add new one to redis cache
            else:
                print('Not in cache')
                result = dateRange(dic)
                r_cache.set(r_key,str(result))
            
            if result==[]:
                result==[]
                flash ('No Such entries in table')
        else:
            result=[]
            flash('Please enter values in fields')
        
    end = time.time()
    print(end-start)
        
    return render_template('index.html', data=result,time=end-start)



if __name__ == "__main__":
    app.run()

