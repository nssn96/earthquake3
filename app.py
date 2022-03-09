# Name : Surya Narayanan Nadhamuni Suresh
# UTA ID : 1001877873

# References used for the Assignment 3

# creating redis cache in azure
# 1) https://www.youtube.com/watch?v=OPGrdzbUpcc

# 2) https://docs.microsoft.com/en-us/azure/azure-cache-for-redis/cache-python-get-started

# 3) execution time in python
# https://www.geeksforgeeks.org/how-to-check-the-execution-time-of-python-script/
# https://stackoverflow.com/questions/1557571/how-do-i-get-time-of-a-python-programs-execution

# 4)
# https://docs.microsoft.com/en-us/azure/azure-cache-for-redis/cache-python-get-started#skip-to-the-code-on-github

# 5)
# https://stackoverflow.com/questions/63074531/integrate-redis-with-flask-application-using-redis-py

# 6)Redis application
# https://realpython.com/python-redis/#using-enterprise-redis-applications

# 7)
# Redis list type
# https://www.youtube.com/watch?v=GBKDbTa0nU0&t=111s

# 8) using redis with flask app
# https://stackoverflow.com/questions/63074531/integrate-redis-with-flask-application-using-redis-py

# 9) https://redis.io/topics/data-types

from flask import Flask,render_template, request,url_for,flash
import mysql.connector as mysql
from geopy.geocoders import Nominatim as nm
from geopy.point import Point
from math import radians, sin, cos,sqrt,atan2
from decimal import Decimal
import time
import redis
import datetime


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
#1
def largestN(fields):
    query="select Name,id from ni where id>="+fields['id1']+"and id<="+fields['id2']+" order by id desc"
    dbConnect()
    cursor = conn.cursor()
    # for key,value in fields.items():
    #     query+="order by mag desc LIMIT 0,"+value
    #     #query+="where mag > "+value+" order by mag desc"
    print(query)
    
    cursor.execute(query)
    res = cursor.fetchall()
    return res
#2
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
#3
def groupByMag(fields):
    query = "select t.new as 'mag_range', count(*) as 'number_of_occurences' from ( "
    query+="select case when mag>=1 and mag<2 then '1-2' "
    query+="when mag>=2 and mag<3 then '2-3' when mag>=3 and mag<4 then '3-4' "
    query+="when mag>=4 and mag<5 then '4-5' when mag>=5 and mag<6 then '5-6' "
    query+="when mag>=6 and mag<7 then '6-7' else 'other(negatives)' end as new "
    query+="from earthquake "
    query+="where date(time)>=date(curdate()-"
    query+=fields['days']+")"
    query+=" ) t group by t.new"
    dbConnect()
    cursor = conn.cursor()
    cursor.execute(query)
    res = cursor.fetchall()
    print(query)
    print(res)
    conn.close()
    return res
#4
def getDistanceData(fields):
    #query= "Select * from earthquake where id IN ("
    dbConnect()
    cursor = conn.cursor()
    # flag=0
    # for value in id:
    #     if flag>0:
    #         query+=","
    #     query+="'"+value+"'"
    #     flag+=1
    # query+=")  order by mag desc "

    loc = getLatLong(fields['location'])
    query= "SELECT time,latitude,longitude,depth,mag,magtype,place, (6371 * acos (cos ( radians("+str(loc.latitude)+") )* cos( radians( latitude ) )* cos( radians( longitude ) - radians("+str(loc.longitude)+") )+ sin ( radians("+str(loc.latitude)+") )* sin( radians( latitude ) ))) AS distance FROM earthquake where (6371 * acos (cos ( radians("+str(loc.latitude)+") )* cos( radians( latitude ) )* cos( radians( longitude ) - radians("+str(loc.longitude)+") )+ sin ( radians("+str(loc.latitude)+") )* sin( radians( latitude ) ))) < "+fields['distance']+" ORDER BY distance desc"


    #print(query)
    cursor.execute(query)
    res = cursor.fetchall()
    count=0
    for i in res:
        count+=1
    print(count)
    conn.close()
    return res

#References used for the below function
#https://www.tutorialspoint.com/how-to-get-the-longitude-and-latitude-of-a-city-using-python
def getLatLong(place):
    #initialize the Nominatim API
    locator = nm(user_agent="MyApp")

    location = locator.geocode(place)
    return location

#https://stackoverflow.com/questions/57770044/valueerror-must-be-a-coordinate-pair-or-point
def getPlaceName(fields):
    #initialize the Nominatim API
    locator = nm(user_agent="MyApp")
    co_ord = '"'+fields['lat']+' , '+fields['long']+'"'
    location = locator.reverse(Point(fields['lat'],fields['long']))
    address = location.raw['address']
    #address is a dictionary with all the details of the lat,long
    #print(address)
    return address
    


@app.route('/')
def index():
    return render_template('index.html')
#references used for REDIS integration
#https://stackoverflow.com/questions/63074531/integrate-redis-with-flask-application-using-redis-py
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
            
            print(result)

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

@app.route('/groupby',methods=['POST','GET'])
def groupBy():
    start = time.time()
    if request.method=='POST':
        dic={}
        r_key=''
        for key,value in request.form.items():
            if value!='':
                dic[key]=value
                r_key+="_"+key+"_"+value
        
        print(r_key)

        if dic:
            #cache hit--> then we can get from redis cache
            if r_cache.exists(r_key):
                print('Present in cache')
                result = eval(r_cache.get(r_key).decode("utf-8"))
            #cache miss--> if not in redis cache,then DB call and add new one to redis cache
            else:
                print('Not in cache')
                result=groupByMag(dic)
                r_cache.set(r_key,str(result))

            if result==[]:
                result=[]
                flash('No records of earthquake for above mentioned days')

        else:
            result=[]
            flash('Please enter values in the field')
        
        end = time.time()

    return render_template('index.html',data2=result,time2=end-start)

@app.route('/distance',methods=['GET','POST'])
def distance():
    start = time.time()
    if request.method=='POST':
        dic={}
        r_key=''
        for key,value in request.form.items():
            if value!='':
                dic[key]=value
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
                result = getDistanceData(dic)
                r_cache.set(r_key,str(result))
        
        if result==[]:
            flash('No records of earthquake around that location for that distance')
            
    else:
        result=[]
        flash('Please enter values in the field')
        
        
    end = time.time()
    
    return render_template('index.html',data3=result,time3=end-start)

# @app.route('/placename',methods=['GET','POST'])
# def placeName():
#     if request.method=='POST':
#         dic={}
#         for key,value in request.form.items():
#             if value!='':
#                 dic[key]=value
        
#         #print(dic)

#         if dic:
#             result=getPlaceName(dic)
#         else:
#             result=[]
#             flash('Please enter values in the field')
    
#     return render_template('index.html',place=result)
    
# @app.route('/latlong',methods=['GET','POST'])
# def latLong():
#     if request.method=='POST':
#         dic={}
#         for key,value in request.form.items():
#             if value!='':
#                 dic[key]=value
        
#         #print(dic)

#         if dic:
#             result=getLatLong(dic['location'])
#             print(result.address)
#         else:
#             result=[]
#             flash('Please enter values in the field')
    
#     return render_template('index.html', loc=result) 

if __name__ == "__main__":
    app.run()

