#!flask/bin/python
# -*- coding: utf-8 -*-
from flask import Flask, jsonify, abort, request
from pymongo import MongoClient
from bson.json_util import dumps
from pprint import pprint

import openstack
import boto3
from getpass   import getpass
from base64    import b64encode

SWITCH_AUTH_URL = 'https://keystone.cloud.switch.ch:5000/v3'
SWITCH_CONTAINER_URL = 'https://engines.switch.ch/horizon/api/swift/containers/%s/object/%s'
SWITCH_REGION = 'LS'

#TODO flip this flag to use OpenStack SWIFT or AWS S3
USE_SWIFT = True

AWS_CONTAINER_URL = 'https://%s.s3.amazonaws.com/%s'

def create_connection_switch(auth_url, project_name, username, password, region): 
    return openstack.connect(
        auth_url=auth_url,
        project_name=project_name,
        username=username,
        password=password,
        region_name=region,
        domain_name='default',
    )

def create_connection_aws(): 
    return boto3.resource('s3')

import os
import json
import sys
import time
import datetime

#Global vars
app = Flask(__name__)
PASSWORD = "labolabo"
USER = "labo"
CLIENT = None
DB = None
COLLECTION = {}
CONTAINER_NAME = 'smarthepia'
ip_addr = sys.argv[1]

if USE_SWIFT:
    openstack_user   = sys.argv[2]
    openstack_tenant = sys.argv[3] # username:tenant
    openstack_pwd    = getpass() # password :)

#openstack_region   = 'RegionOne' # default region of HepiaCloud
#openstack_auth_url = 'http://hepiacloud.hesge.ch:5000/v3'

def cleanIds(t):
    for el in t:
        del el["_id"]
        return t

def getControllersTable(controller):
    try:
        return COLLECTION[controller]
    except:
        return None

@app.route('/getLastSensorValue/<string:controller>/<int:sensor_id>', methods=['GET'])
def routeGetLast(controller,sensor_id):
    return dumps(cleanIds(list(getLastSensorValue(controller,sensor_id))))

@app.route('/getLastSensorsValues/<string:controller>', methods=['GET'])
def routeGetLastValues(controller):
    if controller=='pi1':
        nb=[2,3,4,5,6,7,14,15,16,17,18,19,28]
    elif controller=='pi2':
        nb=[2,3,4,5,6,7,8,9,10,11]
    else:
        nb=[2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21]
    list1="["+dumps(cleanIds(list(getLastSensorValue(controller,nb[0]))))
    for sensor in nb[1:]:
        list1+=","+dumps(cleanIds(list(getLastSensorValue(controller,sensor))))
    return list1+"]"

@app.route('/getSensorValuesInLastHours/<string:controller>/<int:sensor_id>/<int:hours>', methods=['GET'])
def routeGetInLastHours(controller,sensor_id, hours):
    return dumps([cleanIds(list(getSensorValuesInLastNHours(controller,sensor_id,hours))[::5]),cleanIds(list(getLastSensorValue(controller,sensor_id)))])

def getSensorValuesBetweenDates(controller, sensor_id, from_date, to_date):
    col = getControllersTable(controller)
    if col == None:
        return []
    from_ts = time.mktime(time.strptime(from_date, '%d-%m-%Y:%H:%M'))
    to_ts   = time.mktime(time.strptime(to_date, '%d-%m-%Y:%H:%M'))
    print(from_ts, to_ts)
    if from_ts >= to_ts:
        return []
    print("""{
        "sid":%s ,
        "ts" : {
            "$gt" : %s,
            "$lt" : %s
            }
    }""" % (sensor_id, from_ts, to_ts))
    return col.find({
        "sid":sensor_id ,
        "ts" : {
            "$gt" : from_ts,
            "$lt" : to_ts
            }
    }).sort("ts", 1)

@app.route('/getSensorValuesBetweenDates/<string:controller>/<int:sensor_id>/<string:from_date>/<string:to_date>', methods=['GET'])
def routeStoreBetweenDateSwift(controller, sensor_id, from_date, to_date):
    object_data = dumps([cleanIds(list(getSensorValuesBetweenDates(controller, sensor_id, from_date, to_date))[::5])])
    object_name = controller+"/"+str(sensor_id)+"/"+from_date+'_to_'+to_date
    if USE_SWIFT:
        return storeDataSwift(object_name, object_data)
    else:
        return storeDataS3(object_name, object_data)

def storeDataSwift(object_name, object_data):
    conn = create_connection_switch(SWITCH_AUTH_URL,openstack_tenant, openstack_user, openstack_pwd, SWITCH_REGION)
    container = conn.create_container(CONTAINER_NAME)
    conn.create_object(container=container.name, name =object_name, data = object_data )
    return f"{SWITCH_CONTAINER_URL}, {container.name}, {object_name})"

def storeDataS3(object_name, object_data):
    #TODO
    #return AWS_CONTAINER_URL % (bucket.name, object_name)
    return "TODO"

def getSensorValuesInLastNHours(controller,sensor_id, nhours):
    col = getControllersTable(controller)
    if col == None:
        return []
    if nhours > 72:
        nhours = 72
    ts = time.time() - nhours*3600
    print(datetime.datetime.fromtimestamp(int(ts)))
    return col.find({
        "sid":sensor_id ,
        "ts" : {
            "$gt" : ts
            }
    }).sort("ts", 1)

def getLastSensorValue(controller,sensor_id):
    col = getControllersTable(controller)
    if col == None:
        return []
    return col.find({"sid":sensor_id}).sort("_id" , -1).limit(1)


@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

if __name__ == '__main__':
    CLIENT = MongoClient(ip_addr, 27017)
    #CLIENT.labo.authenticate(USER,PASSWORD)
    DB = CLIENT.smarthepia

    #DB COLLECTIONS WHITELIST
    COLLECTION["pi1"] = DB.pi1
    COLLECTION["pi2"] = DB.pi2
    COLLECTION["pi3"] = DB.pi3

#    context = ('server.cert', 'server.key')
    app.run(debug=False,host='0.0.0.0',port=18000,threaded=True)
    '''
            #24h -> 60kb w/ ids, 40 w/o
            #48h -> 120kb w/ids, 80 w/o
            # => ids en string = 33% string length
            a = list(getLastSensorValue("9"))[0]
            print("Last value : ")
            print(str(a["ts"]) +" "+ str(datetime.datetime.fromtimestamp(int(a["ts"]))))

            b = list(getSensorValuesInLastNHours("6",48))
            print(len(b))
            print(sys.getsizeof(dumps(b)))
            print(sys.getsizeof(dumps(cleanIds(b))))

            for i in b:
            print(str(int(i["ts"]))+ " "+ str(datetime.datetime.fromtimestamp(int(i["ts"]))))
    '''
