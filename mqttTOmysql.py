#!/usr/bin/env python
# September 2013
# by Matthew Bordignon, @bordignon on Twitter
#
# Simple Python script (v2.7x) that subscribes to a MQTT broker topic and inserts the topic into a mysql database
# This is designed for the http://mqttitude.org/ project backend
#

import MySQLdb
import mosquitto
import json
import time

config = json.load(open('./mqttitudeTOmysql.conf'))

broker_topic = '#'

# Open database connection
db = MySQLdb.connect(config['mysql_server'], config['mysql_username'], config['mysql_passwd'], config['mysql_db'])
# prepare a cursor object using cursor() method
cursor = db.cursor()

def on_connect(mosq, obj, rc):
    if config['debug']: print("rc: "+str(rc))

def on_message(mosq, obj, msg):
    if config['debug']: print(msg.topic+" "+str(msg.qos)+" "+str(msg.payload))

    try:
       # Execute the SQL command 
       # change locations to the table you are using
       queryText = "INSERT INTO %s ( message, topic, qos ) VALUES ( '%s', '%s', %d )"
       queryArgs = ( tuple([ config['mysql_table'], MySQLdb.escape_string(msg.payload), MySQLdb.escape_string(msg.topic), msg.qos ]))
       cursor.execute(queryText % queryArgs)
       if config['debug']: print('Successfully Added record to mysql')
       db.commit()
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
        except IndexError:
            print "MySQL Error: %s" % str(e)
        # Rollback in case there is any error
        db.rollback()
        print('ERROR adding record to MYSQL')

def on_publish(mosq, obj, mid):
    print("mid: "+str(mid))

def on_subscribe(mosq, obj, mid, granted_qos):
    if config['debug']: print("Subscribed: "+str(mid)+" "+str(granted_qos))

def on_log(mosq, obj, level, string):
    if config['debug']: print(string)

mqttc = mosquitto.Mosquitto(config['broker_clientid'], clean_session=True )
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish
mqttc.on_subscribe = on_subscribe

# Uncomment to enable config['debug'] messages
mqttc.on_log = on_log

mqttc.username_pw_set(config['broker_username'], password = config['broker_password'])

mqttc.connect(config['broker'], config['broker_port'], 60)
mqttc.subscribe(broker_topic, 0)

rc = 0
while rc == 0:
    rc = mqttc.loop()

if config['debug']: print("rc: "+str(rc))

# disconnect from server
print ('Disconnected, done.')
db.close()
