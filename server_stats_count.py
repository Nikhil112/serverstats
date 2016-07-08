#!/usr/bin/python
# -*- coding: utf-8 -*-
from influxdb import InfluxDBClient
import datetime
import time
import MySQLdb
import unidecode
import ConfigParser
import urllib2
import logging
import glob
import os
import logging.handlers

filepath = '/home/platform/serverdata_apps/serverstat_new/serverstats_credential.cfg'
if not os.path.exists(filepath):
	raise IOError('File does not exist: %s' % filepath)

config = ConfigParser.ConfigParser()
config.read(filepath)

# Getting Logger credential from config file
LOG_FILENAME = config.get('logger_detail', 'filename')
get_logger = logging.getLogger('MyLogger')

try:
	log_level = config.get('logger_detail', 'logger_level')
	if log_level == 'debug':
		get_logger.setLevel(logging.DEBUG)
	else:
		get_logger.setLevel(logging.INFO)
	# handler = logging.handlers.RotatingFileHandler(
	              # LOG_FILENAME, maxBytes=20000000, backupCount=2)
	handler_when = config.get('logger_detail', 'when')
	handler_interval = config.get('logger_detail', 'interval')
	handler_backupCount = config.get('logger_detail', 'backupCount')
	handler_encoding = config.get('logger_detail', 'encoding')
	handler_delay = config.get('logger_detail', 'delay')
	handler_utc = config.get('logger_detail', 'utc')

	handler = logging.handlers.TimedRotatingFileHandler(LOG_FILENAME, when=handler_when, interval=int(handler_interval), 
				backupCount=int(handler_backupCount), encoding=None, delay=int(handler_delay), utc=bool(int(handler_utc)))
	fmt = logging.Formatter('%(asctime)s %(filename)s:%(lineno)d %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
	handler.setFormatter(fmt)
	get_logger.addHandler(handler)

	get_logger.info('starting process')
	# Open database connection
	MySQL_host = config.get('MySQL_credential', 'host')
	MySQL_user = config.get('MySQL_credential', 'user')
	MySQL_password = config.get('MySQL_credential', 'password')
	MySQL_DBName = config.get('MySQL_credential', 'database_name')

	get_logger.debug("connecting MySQL database ->" + MySQL_host +','+ MySQL_user +','+ MySQL_password +','+ MySQL_DBName)

	db = MySQLdb.connect(MySQL_host,MySQL_user,MySQL_password,MySQL_DBName)

	# prepare a cursor object using cursor() method
	cursor = db.cursor()

	sql = "SELECT id, name from exchanges;"
	cursor.execute(sql)
	exchange_data = cursor.fetchall()

	sql2 = "SELECT id, name from countries;"
	cursor.execute(sql2)
	country_data = cursor.fetchall()

	get_logger.info('fetched data from MySQL')



	get_logger.info('fetching web id from config')

	dca_string = config.get('web_machine', 'serverlist_dca')
	hkg_string = config.get('web_machine', 'serverlist_hkg')

	server_dca = dca_string.split(',')
	server_hkg = hkg_string.split(',')
	#print server_dca
	#print server_hkg
	get_logger.info('serverlist loaded')
	response_table_dca = {}
	response_table_hkg = {}
	server_hr = str(datetime.datetime.now().time()).split(':',2)[0]

	get_logger.info('start fetching data from each web machine')
	# if not session.server_hr or session.server_hr != server_hr:
	#   session.server_hr = server_hr
	for server in server_dca:
		get_logger.debug("server -> " + server)
		params = server.split(':')
		# print params[0]
		request = urllib2.Request("http://"+ params[1] +":3639/exstat")
		try: 
		    response_table_dca[params[0]] = urllib2.urlopen(request).read()
		except urllib2.HTTPError as e:
		    get_logger.error(e.reason)
		except urllib2.URLError as e:
		    get_logger.error(e.reason)
		except httplib.HTTPException, e:
		    get_logger.error(e.reason)
		except Exception:
			import traceback
	     	get_logger.error('generic exception: ' + traceback.format_exc())
	for server in server_hkg:
		get_logger.debug("server -> " + server)
		params = server.split(':')
		request = urllib2.Request("http://"+ params[1] +":3639/exstat")
		try: 
		    response_table_hkg[params[0]] = urllib2.urlopen(request).read()
		except urllib2.HTTPError as e:
		    get_logger.error(e.reason)
		except urllib2.URLError as e:
		    get_logger.error(e.reason)
		except httplib.HTTPException, e:
		    get_logger.error(e.reason)
		except Exception:
			import traceback
	     	get_logger.error('generic exception: ' + traceback.format_exc())

	#for server in response_table_dca:
	#	print (server, response_table_dca[server])
	#print response_table_dca
	#print response_table_hkg

	get_logger.info('finished fetching data')

	table1 = {}

	def getDictinory(response_table, server_name):
		for server in response_table:
			response = response_table[server]
			data = response.split('\n')
			if table1.has_key(server) == False:
				table1[server] = {}
			for i in data:
				pair = i.split(' : ')
				if pair[0] and len(pair[0]) > 1:
					params = pair[0].split('-')
					if table1[server].has_key(params[0]) == False:
						table1[server][params[0]] = {}
					if table1[server][params[0]].has_key(params[5]) == False:
						table1[server][params[0]][params[5]] = {}
					if params[0] == 'REC':
						time_hr = params[1]+':'+params[2]+':'+params[3]+':'+params[4]
						if table1[server][params[0]][params[5]].has_key(time_hr) == False:
							table1[server][params[0]][params[5]][time_hr] = {}
							table1[server][params[0]][params[5]][time_hr]['server_type'] = server_name
							table1[server][params[0]][params[5]][time_hr]['count'] = int(pair[1])
							table1[server][params[0]][params[5]][time_hr]['qps'] = int(pair[1])/3600.0
					else:
						if table1[server][params[0]][params[5]].has_key(params[6]) == False:
							table1[server][params[0]][params[5]][params[6]] = {}
						time_hr = params[1]+':'+params[2]+':'+params[3]+':'+params[4]
						if table1[server][params[0]][params[5]][params[6]].has_key(time_hr) == False:
							table1[server][params[0]][params[5]][params[6]][time_hr] = {}
							table1[server][params[0]][params[5]][params[6]][time_hr]['server_type'] = server_name 
							table1[server][params[0]][params[5]][params[6]][time_hr]['count'] = int(pair[1])
							table1[server][params[0]][params[5]][params[6]][time_hr]['qps'] = int(pair[1])/3600.0


	exchange_table = {}
	for id,name in exchange_data:
	  	exchange_table[str(id)] = name
	exchange_table['0'] = 'Unknown'

	country_table = {}
	for id,name in country_data:
		# name = unicode(name, errors='replace')
		name = unidecode.unidecode_expect_nonascii(name)
		# name = name.decode("utf-8", "replace")
	  	country_table[str(id)] = name
	country_table['0'] = 'Unknown'

	getDictinory(response_table_dca, 'dca')
	getDictinory(response_table_hkg, 'hkg')

	get_logger.info('inserting data into influxdb')
	points = []
	now = datetime.datetime.today()
	for i in table1:
		for j in table1[i]:
			if j == 'REC':		
				for k in table1[i][j]:
					for l in table1[i][j][k]:
						point = {
							"measurement" : 'server_stats_table',
							"time": int(time.mktime(datetime.datetime.strptime(l, "%Y:%m:%d:%H").timetuple()))*1000000000,
							"tags": {
								"server" : i,
								"type" : j,
								"server_name" : table1[i][j][k][l]['server_type'],
								"exchange" : exchange_table[k],
								"country" : country_table['0']
							},
							"fields": {
								"count" : table1[i][j][k][l]['count'],
								"qps" : table1[i][j][k][l]['qps']
							}
						}
						points.append(point)
			else:
				for k in table1[i][j]:
					for l in table1[i][j][k]:
						for p in table1[i][j][k][l]:
							point = {
								"measurement" : 'server_stats_table',
								"time" : int(time.mktime(datetime.datetime.strptime(p, "%Y:%m:%d:%H").timetuple()))*1000000000,
								"tags" : {
									"server" : i,
									"type" : j,
									"server_name" : table1[i][j][k][l][p]['server_type'],
									"exchange" : exchange_table[k],
									"country": country_table[l]
								},
								"fields":{
									"count" : table1[i][j][k][l][p]['count'],
									"qps" : table1[i][j][k][l][p]['qps']
								}
							}
							points.append(point)

	Influx_USER = config.get('Influxdb_credential', 'user')
	Influx_PASSWORD = config.get('Influxdb_credential', 'password')
	Influx_DBNAME = config.get('Influxdb_credential', 'database_name')
	Influx_host = config.get('Influxdb_credential', 'host')
	Influx_port = config.get('Influxdb_credential', 'port')

	get_logger.debug("Inserting data to ->" + Influx_host +','+ Influx_port +',' + Influx_USER +','+ Influx_PASSWORD +','+ Influx_DBNAME)

	client = InfluxDBClient(Influx_host, Influx_port, Influx_USER, Influx_PASSWORD, Influx_DBNAME)

	client.create_database(Influx_DBNAME)
	client.switch_database(Influx_DBNAME)

	    #Write points
	client.write_points(points)
	get_logger.info('finished data')

except Exception as e:
	get_logger.error(e.reason)


#query = 'SELECT * FROM server_stats_table'
#print("Queying data: " + query)
#result = client.query(query, database=DBNAME)
#print("Result: {0}".format(result))
