import sys
import json
import bson
from datetime import datetime, timedelta, date
import logging
import pymongo as mongo
import dateutil.parser
from pytz import timezone
import pytz
import tzlocal
import time

# GET MONGO DB DATABASE
def getDB(configuration):
    # establish mongodb connection here
    logging.info("Connection to MongoDB")
    database = ""

    try:
        conx = mongo.MongoClient(configuration["mongodbUserPass"])
        database = conx["amps_metrics"]
    except mongo.errors.ConnectionFailure as e:
        logging.error("MongoDB connection failed: " + str(e))
        print("MongoDB connection failed: check logs")
        sys.exit()

    logging.info("MongoDB connection successful")
    return database


# GET COUNT OF UNIQUE USERS BASED ON INPUTTED TIMEFRAME
def countOfUniqueUsers(db, daysc, sdate):

    deltaCount = timedelta(days=daysc)
    delta1 = timedelta(days=1)
    currDate = datetime.strptime(sdate, '%Y%m%d')

    dobjCount = datetime.strptime(sdate, '%Y%m%d').date() - deltaCount
    dobj2 = datetime.strptime(sdate, '%Y%m%d').date() + delta1

    prevDays = datetime(dobjCount.year, dobjCount.month, dobjCount.day)
    nextDate = datetime(dobj2.year, dobj2.month, dobj2.day)

    # Make daylight savings adjustment to times
    local_timezone = tzlocal.get_localzone()
    now = datetime.now()
    timestamp1 = int(now.timestamp())
    isdst = datetime.fromtimestamp(timestamp1, local_timezone).dst()
    seven = timedelta(hours=7)
    eight = timedelta(hours=8)
    
    if isdst:
        hrsToAdd = seven
    else:
        hrsToAdd = eight

    prevDays = prevDays + hrsToAdd
    nextDate = nextDate + hrsToAdd

    logging.info("Obtaining unique user count for previous " + str(daysc) + " from " + sdate + ".")
    monthlyUsersResult = db.amps_logs.find({'metadata.timestamp' : {'$gte': prevDays, '$lt': nextDate}}).distinct('metadata.userInfo.name')
    monthlyUsers = list(monthlyUsersResult)
    usersCount = len(monthlyUsers)
    logging.info("MongoDB query returned successfully")

    return usersCount


# INSERT UNIQUE USER COUNT FOR EACH TIMEFRAME
def insertUserMetrics(db, sdate, daysCount, ucount):
    # insert metrics into metrics collection
    coll = db.amps_metrics_new
    sdate_obj = datetime.strptime(sdate, "%Y%m%d").date()
    isoDate = datetime(sdate_obj.year, sdate_obj.month, sdate_obj.day)

    # format attributes
    metricname = "unique_users_count"
    metricgroup = "unique_users"
    metrictype = "count"
    ucount = int(ucount)
    daysCount = str(daysCount) + " days"

    logging.info("Inserting user unique count metric into metrics database")
    
    try:
        document = {  "logType" : "wize_log",
                      "date" : isoDate,
                      "metricName" : metricname,
                      "metricGroup" : metricgroup,
                      "timeframe" : daysCount,
                      "value" : ucount,
                      "type" : metrictype
                    } 
        coll.insert(document)
    
    except IOError as e:
        logging.error("Metrics insertion failed: " + e.strerror)
        logging.error("Exception for " + src + " : " + str(e.errno) + " " + e.strerror)        


# FINISHUP FUNCTION PRINTS FINAL LOGGING STATEMENT
def finishUp():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info("- - - - - - - - - - - FINISHED insertUserStatistics.py at " + timestamp + " - - - - - - - - - - -")


# MAIN FUNCTION, CALLS COUNTOFUNIQUEUSERS FOR 1,7,14, AND 31 DAYS PRIOR TO CURRENT OR INPUTTED DATE
def main():
    db = getDB(configList)
    
    # Get previous 1 day count
    prev1 = countOfUniqueUsers(db, 1, sdate)
    insertUserMetrics(db, sdate, 1, prev1)

    # Get previous 7 days count
    prev7 = countOfUniqueUsers(db, 7, sdate)
    insertUserMetrics(db, sdate, 7, prev7)

    # Get previous 14 days count
    prev14 = countOfUniqueUsers(db, 14, sdate)
    insertUserMetrics(db, sdate, 14, prev14)

    # Get previous 31 days count
    prev31 = countOfUniqueUsers(db, 31, sdate)
    insertUserMetrics(db, sdate, 31, prev31) 

    finishUp()


# START PROGRAM HERE
if __name__ == "__main__": 
    delta = timedelta(days=1)
    sdate_obj = date.today() - delta
    sdate = sdate_obj.strftime('%Y%m%d')

    if len(sys.argv) < 2:
        print("Missing configuration file: Program exiting")
        sys.exit()

    if len(sys.argv) == 3:
        sdate = sys.argv[2]

    # Unpack and scan config file
    config_file = open(sys.argv[1], encoding='utf-8')
    config = json.load(config_file)
    configList = config.pop(0)
    loggingDest = configList["logLocation"]
    
    if loggingDest[-1] != "/":
        loggingDest += "/"
    
    loggingDest += configList["logfile"]
    logging.basicConfig(filename=loggingDest,
                        level=logging.DEBUG, format=configList["loggingFormat"])
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
    
    logging.info("- - - - - - - - - - STARTED insertUserStatistics.py at " + timestamp + " - - - - - - - - - -")
    main()



