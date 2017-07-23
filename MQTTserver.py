
import MySQLdb
import paho.mqtt.client as mqtt
import json

broker = 'iot.eclipse.org'
broker_port = 1883
broker_topic = 'dashboard/device1'

mysql_server = '127.0.0.1'
mysql_username = 'pushpa'
mysql_password = '1234'


def on_connect(client, userdata, flags, rc):
    print("on_connect--> result code: "+str(rc))
    client.subscribe(broker_topic)

def on_message(client, userdata, msg):
    print("on_message-->"+msg.topic+" "+str(msg.qos)+" "+str(msg.payload))
    vars_to_sql = []
    keys_to_sql = []
        
    validate = False
    list = []
    time = 0

    print "topic:", msg.topic
    array = msg.topic.split('/')
    dbName = array[1]
    
    list = json.loads(msg.payload)
    
    for key,value in list.iteritems():
        print ("")
        print key, value
        if(key == "timeStamp"):
            validate = True
            time = str(value)
        else:
            value_type = type(value)
            if value_type is unicode:
                print "value_type is unicode"
                vars_to_sql.append(value.encode('ascii', 'ignore'))
                keys_to_sql.append(key.encode('ascii', 'ignore'))
            else:
                print "value_type is not unicode"
                vars_to_sql.append(str(value))
                keys_to_sql.append(key)
    if (validate):
        mysql_db = 'dashboard'
        db = MySQLdb.connect(mysql_server, mysql_username, mysql_password, mysql_db)
        cursor = db.cursor()
        #check the database exists or not. if not create it
        checkDataBase(db, cursor, dbName)
        db.close()
        mysql_db = dbName
        db = MySQLdb.connect(mysql_server, mysql_username, mysql_password, mysql_db)
        cursor = db.cursor()

        for i in range(0,len(keys_to_sql)):
            keys_to_table = []
            keys_to_table.append("id INT(11) PRIMARY KEY AUTO_INCREMENT")
            keys_to_table.append("timeStamp VARCHAR(10)")
            keys_to_table.append(keys_to_sql[i]+" VARCHAR(5)")
            keys_to_table = ', '.join(keys_to_table)
            
            #check the database exists or not. if not create it
            checkTable(db, cursor, keys_to_sql[i], keys_to_table)
            
            keys = []
            keys.append("timeStamp")
            keys.append(keys_to_sql[i])
            keys = ', '.join(keys)

            print keys
            
            values = []
            values.append(time)
            values.append(vars_to_sql[i])

            print values
        
            try:
               queryText = "INSERT INTO %s(%s) VALUES %r"
               queryArgs = (keys_to_sql[i],keys, tuple(values))
               cursor.execute(queryText % queryArgs)
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
        db.close()
    else:
        print "Data is not valid"
    
def checkDataBase(db, cursor, dbName):
    cursor.execute("SET sql_notes = 0; ")
    queryText = "CREATE DATABASE IF NOT EXISTS %s"
    queryArgs = (dbName)
    cursor.execute(queryText % queryArgs)
    db.commit()
def checkTable(db, cursor, tbName, keys_to_table):
    cursor.execute("SET sql_notes = 0; ")
    queryText = "CREATE TABLE IF NOT EXISTS %s(%s)"
    queryArgs = (tbName, keys_to_table)
    cursor.execute(queryText % queryArgs)
    db.commit()

def on_publish(client, userdata, mid):
    print("on_publish--> mid: "+str(mid))

def on_subscribe(client, userdata, mid, granted_qos):
    print("on_subcribe--> Subscribed: "+str(mid)+" "+str(granted_qos))

def on_log(client, userdata, level, buf):
    print("on_log--> buf:"+str(buf))


client = mqtt.Client()
client.on_message = on_message
client.on_connect = on_connect
client.on_publish = on_publish
client.on_subscribe = on_subscribe
client.on_log = on_log

client.connect(broker, broker_port, 60)
client.subscribe(broker_topic, 0)

rc = 0
while rc == 0:
    rc = client.loop()

print("rc: "+str(rc))

# disconnect from server
print ('Disconnected, done.')

