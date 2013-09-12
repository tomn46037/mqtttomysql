#!/usr/bin/env python
# September 2013
# by Matthew Bordignon, @bordignon on Twitter
#
# Simple Python script (v2.7x) that subscribes to a MQTT broker topic and inserts the topic into a mysql database
# This is designed for the http://mqttitude.org/ project backend
#

import MySQLdb

#mysql config
mysql_server = 'mysqlserver.localdomain'
mysql_username = 'root'
mysql_passwd = ''
mysql_db = 'mqtt'
#change table below.

# Open database connection
db = MySQLdb.connect(mysql_server, mysql_username, mysql_passwd, mysql_db)

# prepare a cursor object using cursor() method
cursor = db.cursor()

#what the JSON format actually looks like
#/test/location {"tst":"1378516391","acc":"1414m","alt":"0.000000","vac":"-1m","lon":"145.342210","vel":"-1.000000","_type":"location","lat":"-37.912095","dir":"-1.000000"}

#what I'm using atm until i work out the /test/location part!
list = {"tst":"1378939033","acc":"10m","alt":"12.133484","vac":"6m","lon":"144.933923","vel":"0.000000","_type":"location","lat":"-37.831871","dir":"6.328125"}

vars_to_sql = []
keys_to_sql = []

for key,value in list.iteritems():
  value_type = type(value)
  if value_type is not dict:
    if value_type is unicode:
      vars_to_sql.append(value.encode('ascii', 'ignore'))
      keys_to_sql.append(key.encode('ascii', 'ignore'))
    else:
      vars_to_sql.append(value)
      keys_to_sql.append(key)
 
keys_to_sql = ', '.join(keys_to_sql)

try:
   # Execute the SQL command 
   # change locations to the table you are using
   queryText = "INSERT INTO locations(%s) VALUES %r"
   queryArgs = (keys_to_sql, tuple(vars_to_sql))
   cursor.execute(queryText % queryArgs)
    
   #I just use this to test that the database is working correctly
   #queryText = "INSERT INTO locations(tst,acc,alt,vac,lon,vel,_type,lat,dir) VALUES ('1378939030','10m','12.133484','6m','144.933923','0.000000','location','-37.831871','6.328125')"
   #cursor.execute(queryText)
   
   print('Successfully Added record to mysql')
   db.commit()
except MySQLdb.Error, e:
    try:
        print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
    except IndexError:
        print "MySQL Error: %s" % str(e)
    # Rollback in case there is any error
    db.rollback()
    print('ERROR adding record to MYSQL')

# disconnect from server
print ('Disconnected, done.')
db.close()