from pymongo import MongoClient
import requests
import time
import logging
import sys
import json


### Version adapted to PI2
### Globals variables ###

servers = ["http://129.194.184.124:5000", "http://129.194.184.125:5000","http://129.194.185.199:5000"]
wait = 4 # number of minutes between each request
logging.basicConfig(filename='client.log',level=logging.WARNING,format='%(asctime)s %(message)s')

### Mongo Config ###
ip_addr = sys.argv[1]
mongoClient = MongoClient(ip_addr, 27017)
db = mongoClient.smarthepia
collection1 = db.pi1
collection2 = db.pi2
collection3 = db.pi3
### Infinite While Loop ###
while True:
	try:
		req_body_pi1 = requests.get(servers[0]+'/sensors')
		body_pi1 = json.loads(req_body_pi1.text).keys()
		for sensor in body_pi1:
			try:
				req = requests.get(servers[0]+'/sensors/'+sensor+'/all_measures')
				if req.status_code == 200 and req.headers['content-type'] == 'application/json' :
					measure = {}
					measure['ts'] = req.json()['updateTime']
					measure['sid'] = req.json()['sensor']
					measure['b'] = req.json()['battery']
					measure['t'] = req.json()['temperature']
					measure['h'] = req.json()['humidity']
					measure['l'] = req.json()['luminance']
					measure['p'] = req.json()['motion']
					measure['room'] = req.json()['location']
					print measure
					collection1.insert_one(measure)
				else :
					#print("Sensor "+str(x)+" is not inserting measure")
					continue
			except Exception:
				logging.warning("Error in PI 1 Network from sensor "+sensor.split('=')[0])
				continue
	except Exception:
		logging.warning("Error cannot connect to PI 1 Network")

	try:
		req_body_pi2 = requests.get(servers[1]+'/sensors')
		body_pi2 = json.loads(req_body_pi2.text).keys()
		for sensor in body_pi2:
			try:
				req = requests.get(servers[1]+'/sensors/'+str(int(sensor.split('=')[0]))+'/all_measures')
				if req.status_code == 200 and req.headers['content-type'] == 'application/json':
					measure = {}
					measure['ts'] = req.json()['updateTime']
					measure['sid'] = req.json()['sensor']
					measure['b'] = req.json()['battery']
					measure['t'] = req.json()['temperature']
					measure['h'] = req.json()['humidity']
					measure['l'] = req.json()['luminance']
					measure['p'] = req.json()['motion']
					measure['room'] = req.json()['location']
                                        print measure
					collection2.insert_one(measure)
				else :
					#print("Sensor "+str(x)+" is not inserting measure")
					continue
			except Exception:
				logging.warning("Error in PI 2 Network from sensor "+sensor.split('=')[0])
				continue
	except Exception:
		logging.warning("Error cannot connect to PI 2 Network")
	
	try:
		req_body_pi3 = requests.get(servers[2]+'/sensors')
		body_pi3 = json.loads(req_body_pi3.text).keys()
		for sensor in body_pi3:
			try:
				req = requests.get(servers[2]+'/sensors/'+str(int(sensor.split('=')[0]))+'/all_measures')
				if req.status_code == 200 and req.headers['content-type'] == 'application/json' :
					measure = {}
					measure['ts'] = req.json()['updateTime']
					measure['sid'] = req.json()['sensor']
					measure['b'] = req.json()['battery']
					measure['t'] = req.json()['temperature']
					measure['h'] = req.json()['humidity']
					measure['l'] = req.json()['luminance']
					measure['p'] = req.json()['motion']
					measure['room'] = req.json()['location']
                                        print measure					
					collection3.insert_one(measure)
				else :
					#print("Sensor "+str(x)+" is not inserting measure")
					continue
			except Exception:
				logging.warning("Error in PI 3 Network from sensor "+sensor.split('=')[0])
				continue
	except Exception:
		logging.warning("Error cannot connect to PI 3 Network")
	
	time.sleep(wait*60)
