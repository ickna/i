#!/usr/bin/python3

###############################################################
# Imports
###############################################################
import random
import subprocess
import urllib.request
import os
import sys
import time
import mysql.connector
import datetime
import worker_info
from hashlib import sha256
import logging



    
###############################################################
#  Variables
###############################################################
taskType = "Undefined"
worker = worker_info.worker

workerConflictRetryTime = 3
numRemoved = 0
numIgnored = 0
numBelowMinSize = 0
printLogToScreen = 0
scriptDir = os.path.dirname(os.path.abspath(__file__))

class tcol:
        magenta = '\033[95m'
        blue = '\033[94m'
        green = '\033[92m'
        yellow = '\033[93m'
        red = '\033[91m'
        white = '\033[97m'
        cyan = '\033[96m'
        ENDC = '\033[0m'
        BOLD = '\033[1m'
        UNDERLINE = '\033[4m'

###############################################################
# Functions
###############################################################
def check_output_dir(f):
    outputDir = os.path.dirname(f)
#   print ('Checking if output folder exist.s..')
    if not os.path.exists(outputDir):
#        print ('Creating output folder.\n')
        os.makedirs(outputDir)
        os.chmod(outputDir, 0o777)
    else:
#        print ('Output folder exists.\n')
        return



def initDl():
        global tmpPath
        global dlPath
        global numRemoved
        global numIgnored
        global numBelowMinSize
        global chunk_size
        global shaSum
        global tries
        global imgUrl
        global logger
        global numOfPics
        global i
        global logPath
        global taskType
        global printLogToScreen

        logPath = os.path.join(scriptDir, "logs", time.strftime("%y.%m.%d"), (str(worker.id) + "." + worker.name), "")
        check_output_dir(logPath)
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        fh = logging.FileHandler(os.path.join(logPath, time.strftime("%H.%M.%S") + '.txt'))
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
##        if printLogToScreen == 1:
##                ch = logging.StreamHandler()
##                ch.setLevel(logging.DEBUG)
##                ch.setFormatter(formatter)
##                logger.addHandler(ch)

        logger.debug("Executing from: %s", scriptDir)
        logger.debug("Logging to: %s", logPath)

        #print("\033c")
        logger.info("Worker ID: " + str(worker.id) + ", Worker name: " + worker.name + ", started via " +taskType)
        #print("Worker ID: " + str(worker.id) + ", Worker name: " + worker.name + ", started via " +taskType)
        db.checkForWorkerConflict(0)
        # Base URL used for downloads
        imgUrl = 'http://i.imgur.com/'

        # output dir for images
        dlPath = os.path.join(scriptDir, 'output', time.strftime("%Y/%m/%d/%H"), "")
        tmpPath = os.path.join(scriptDir, 'tmp')
        logger.info('Output path: ' + dlPath)
        logger.info('Temp file path: ' + tmpPath)

        # sha256sum of placeholder image from imgur.com
        shaSum = '9b5936f4006146e4e1e9025b474c02863c0b5614132ad40db4b925a10e8bfbb9'

        # Number of tries used to fetch an image
        tries = 1
        i = 0
        db.WorkerStatusUpdate("Initializing", 0, numOfPics)
        db.workerMessage(str(worker.id) + ": " + worker.name + " started via " + taskType + ", targeting " + str(numOfPics) + " new images")
        chunk_size = 1048576
        numRemoved = 0
        numIgnored = 0
        numBelowMinSize = 0



        print(tcol.magenta + 'imgur random downloader', tcol.ENDC)
        check_output_dir(tmpPath)
        check_output_dir(dlPath)
        print (tcol.blue, 'Downloading', numOfPics, 'images', tcol.ENDC)
        logger.debug('inintDl finished')

class db:
    def queryWorkerStatus():
        logger.debug('queryWorkerStatus')
 
        cnx = mysql.connector.connect(**worker.dbconfig)
        cursor = cnx.cursor()
        cursor.execute("SELECT worker_status, last_updated FROM workers WHERE worker_id = " + str(worker.id))
        workerStatus = cursor.fetchone()
        cursor.close()
        cnx.close()
        return workerStatus

            
    def checkForWorkerConflict(attempt):
        global workerConflictRetryTime
        timestamp = datetime.datetime.now()
        workerStatus = db.queryWorkerStatus()
        
        if worker.id == 1:
            return
        if workerStatus is None:
            logger.debug('Worker ID does not exist in DB, proceeding')
            return
        else:
            lastWorkerUpdate = workerStatus[1]
            expireDelta = timestamp - lastWorkerUpdate
            logger.debug("Time since worker last updated DB: " + str(expireDelta))
            if expireDelta.seconds >= 120:
                msgTimeout = ("Worker ID "+ str(worker.id) + ": " + worker.name + " update interval timed out, assuming dead process or db connection")
                logger.debug(msgTimeout)
                return
                
            if attempt == 14:
                msgTimeout = ("Worker ID "+ str(worker.id) + ": " + worker.name + " in use, retry timed out after " + str(workerConflictRetryTime * 15) + " seconds")
                db.workerMessage(msgTimeout)
                logger.debug(msgTimeout)
                quit()
                
            if workerStatus[0] != "Finished":
                if (workerStatus[0] == "User Aborted") or () or (workerStatus[0] == "Error"):
                    logging.info('Worker status: %s, continuing', workerStatus[0])
                    pass
                else:
                    msg = ("Worker ID "+ str(worker.id) + ": " + worker.name + " in use (" + workerStatus[0] + "), retrying every " + str(workerConflictRetryTime) + " seconds, max " + str(workerConflictRetryTime * 15))
                    if attempt == 0:
                        db.workerMessage(msg)
                    logger.debug(msg)
                    time.sleep(workerConflictRetryTime)
                    attempt +=1
                    db.checkForWorkerConflict(attempt)
                
    def dbErr(err):
                logger.critical('Database access error')
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                        logger.debug("DB access denied")
                        raise
                elif err.errno == errorcode.ER_BAD_DB_ERROR:
                        logger.debug("Database does not exist")
                        raise
                else:
                        logger.debug(err)
                        raise
            
    def workerMessage(msg):
        global taskType
        logger.info('Sending worker message to DB: ' + msg)
        try:
            timestamp = datetime.datetime.now()
            cnx = mysql.connector.connect(**worker.dbconfig)
            cursor = cnx.cursor()
            workerMsg = "INSERT INTO messages (ts, msg) VALUES (%s, %s)"
            msgData = (str(timestamp), msg)
            
            cursor.execute(workerMsg, msgData)
            cnx.commit()
            cursor.close()
            cnx.close()
        except mysql.connector.Error as err:
            cnx.close()
            db.dbErr(err)
        else:
            cnx.close()
        
    def WorkerStatusUpdate(workerStatus, workerProgress, workerTarget):
        global numRemoved
        global numIgnored
        global numBelowMinSize
        try:
            cnx = mysql.connector.connect(**worker.dbconfig)
            cursor = cnx.cursor()

            timestamp = datetime.datetime.now()
            worker_update = ("REPLACE INTO workers "
                       "(worker_id, worker_name, worker_status, worker_target, worker_progress, last_updated, r, m, i) "
                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
            worker_update_data = (worker.id, worker.name, workerStatus, workerTarget, workerProgress, timestamp, numRemoved, numBelowMinSize, numIgnored)
            logging.info('Sending worker status update to DB: ' + str(worker_update_data[2]))
            cursor.execute(worker_update, worker_update_data)
            cnx.commit()

            cursor.close()
            cnx.close()    
          
        except mysql.connector.Error as err:
            cnx.close()
            db.dbErr(err)
        else:
            cnx.close()

    def UpdateHashTable(hash_val, img_filename):
        logging.info('Sending hash_table DB update:')
        logging.info(img_filename + " with " + hash_val)
        try:
            cnx = mysql.connector.connect(**worker.dbconfig)
            cursor = cnx.cursor()
            timestamp = datetime.datetime.now()
            worker_update = ("INSERT INTO hash_table "
                       "(hash_val, filename, first_seen, first_seen_by) "
                       "VALUES (%s, %s, %s, %s)")
            worker_update_data = (hash_val, img_filename, timestamp, worker.name)
            cursor.execute(worker_update, worker_update_data)
            cnx.commit()

            cursor.close()
            cnx.close()    
          
        except mysql.connector.Error as err:
            cnx.close()
            db.dbErr(err)
        else:
            cnx.close()

    #check a filename against the DB
    def checkFilename(filename):
        try:

            cnx = mysql.connector.connect(**worker.dbconfig)
            cursor = cnx.cursor()

            cursor.execute("SELECT file_name FROM seen_files WHERE file_name = '" + str(filename) + "' ORDER BY id LIMIT 0, 1")
            fnRet = cursor.fetchone()
            cursor.close()
            cnx.close()
            if fnRet is not None:
                return fnRet
          
        except mysql.connector.Error as err:
            cnx.close()
            db.dbErr(err)
        else:
            cnx.close()
            
    #check a hash value against the DB
    def checkHash(hashchk):
        logging.info('Checking for hash value in DB:')
        logging.info(hashchk)
        try:

            cnx = mysql.connector.connect(**worker.dbconfig)
            cursor = cnx.cursor()
            cursor.execute("SELECT * FROM hash_table WHERE hash_val = '" + str(hashchk) + "' ORDER BY id LIMIT 0, 1")
            hashRet = cursor.fetchone()
            cursor.close()
            cnx.close()
            if hashRet is not None:
                return hashRet
            else:
                return None
          
        except mysql.connector.Error as err:
            cnx.close()
            db.dbErr(err)
        else:
            cnx.close()

    # Update the list of previously seen file names
    def UpdateSeenFiles(img_filename, fileSize, isRemoved, isDupe, dupeOf):
        logging.info('Updating seen_files DB with ' +  img_filename)
        try:
            cnx = mysql.connector.connect(**worker.dbconfig)
            cursor = cnx.cursor()            
            timestamp = datetime.datetime.now()
            worker_update = ("INSERT INTO seen_files "
                       "(file_name, isDupe, dupeOf, firstSeen, seenBy, fileSize, isRemoved) "
                       "VALUES (%s, %s, %s, %s, %s, %s, %s)")
            worker_update_data = (img_filename,  isDupe, dupeOf, timestamp, worker.name, fileSize, isRemoved)
            cursor.execute(worker_update, worker_update_data)
            cnx.commit()

            cursor.close()
            cnx.close()    
          
        except mysql.connector.Error as err:
            cnx.close()
            db.dbErr(err)
        else:
            cnx.close()
            


def fetchImg():
    global delayTime

    rTFull = randomnes()
    time.sleep(delayTime) #rate limiting
    local_file_name = downloadImages(rTFull[0], tmpPath, rTFull[1])
    fileSize = getsize(local_file_name)
    logging.info('Fetched %s, %.2f Kb', local_file_name, (fileSize / 1000))
    check_sha256sum(local_file_name, shaSum, numOfPics, tries, rTFull[2], fileSize)

def getsize(filename):
    """Return the size of a file, reported by os.stat()."""
    return os.stat(filename).st_size

def randomnes():	# Function to generate random url to images on imgur.com
    ext = '.jpg'
    chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    
    r1 = random.choice(chars)
    r2 = random.choice(chars)
    r3 = random.choice(chars)
    r4 = random.choice(chars)
    r5 = random.choice(chars)
    rT = r1 + r2 + r3 + r4 + r5
 
    rTE = rT + ext
    rTFull = imgUrl + rTE

    return (rTFull, rTE, rT)


def downloadImages(rTFull, tmpdlPath, filename):	# Download random generatet image url to ./output folder
    local_file_name = os.path.normpath(os.path.join(tmpdlPath, filename))

    with open(local_file_name, 'wb') as f:
        r = urllib.request.urlopen(rTFull).read()
        f.write(r)
        f.close()
        
    return local_file_name

def check_sha256sum(f, shaSum, numOfPics, tries, rT, fileSize):	# Check sha256sum of downloaded image, check if dead image placeholder downloadet, if so download a replacement image.
    global numRemoved
    global numIgnored
    global numBelowMinSize
    global minSize
    global dlPath

    file_name = f
    file_sha256_checksum = sha256()
    try:
        with open(file_name, "rb") as f:
            byte = f.read(chunk_size)
            previous_byte = byte
            byte_size = len(byte)
            file_read_iterations = 1
            while byte:
                file_sha256_checksum.update(byte)
                previous_byte = byte
                byte = f.read(chunk_size)
                byte_size += len(byte)
                file_read_iterations += 1
    except IOError:
        logging.critical('File could not be opened: %s', input_file_name)
        #exit()
        raise
        return
    except:
        raise
    output = file_sha256_checksum.hexdigest()
    
    newShaSum = output[:64]	# grap 64 chars of the shasum


    # check if new file is a placeholder
    
    
    if newShaSum == shaSum:
        # Delete image
        os.remove(file_name)
        shortFileName = file_name[-9:]
        logging.warning("%s is the removed placeholder, deleting", shortFileName)
        numRemoved += 1
        db.UpdateSeenFiles(rT, fileSize, 1, 0, "none")
        # Fetch new image
        fetchImg()
        return


    if getsize(file_name) < minSize:
        # Delete image
        os.remove(file_name)
        shortFileName = file_name[-9:]
        logging.warning("%s is below minimum file size of %.2f Kb, deleting", shortFileName, (minSize / 1000))
        numBelowMinSize += 1
        db.UpdateSeenFiles(rT, fileSize, 0, 0, "none")

        # Fetch new image
        fetchImg()
        return
        
    hashDb = db.checkHash(newShaSum)
    if hashDb is not None:
        
        # Delete image
        os.remove(file_name)
        shortFileName = file_name[-9:]
        logging.warning("Hash match: %s was first seen by %s as %s on %s", file_name, hashDb[4],  hashDb[2],  str(hashDb[3]))
        numIgnored += 1

        db.UpdateSeenFiles(rT, fileSize, 0, 1, newShaSum)
        # Fetch new image
        fetchImg()
        return

    shortFileName = file_name[-9:]
    os.rename(file_name, os.path.normpath(os.path.join(dlPath, shortFileName)))
    logging.info('%s passes criteria, moving to output folder', shortFileName)
    os.chmod(os.path.normpath(os.path.join(dlPath, shortFileName)), 0o777)
    db.UpdateHashTable(newShaSum, rT)
    db.UpdateSeenFiles(rT, fileSize, 0, 0, "none")


    return




def cleanUpAndQuit(exitState):
    global i
    global logPath
    logging.debug("cleanUpAndQuit")
    
    db.WorkerStatusUpdate(exitState, i+1, numOfPics)
    logging.info(exitState + ": " + str(i+1) + " of " + str(numOfPics))
    logging.info("Removed: %i, Ignored: %i, < Min size: %i", numRemoved, numIgnored, numBelowMinSize)
    db.workerMessage(str(worker.id) + ": " + worker.name + " completed " +  str((i+1)) + ' of ' + str(numOfPics))   
    print("Exiting:", exitState)
    logging.info("Exiting: %s", exitState)
    quit()

def downloadLoop():
    global numOfPics
    global hashAutoSave
    global i
    global numRemoved
    global numIgnored
    global numBelowMinSize
    logging.debug('entering downloadLoop')
    initDl()    
    try:
        for i in range(numOfPics):
            db.WorkerStatusUpdate("Downloading", i+1, numOfPics)
            fetchImg()

        cleanUpAndQuit("Finished")
    except KeyboardInterrupt:
        print(tcol.red, "\n\n\n\n\n\n\n\nInterrupt received, stopping", tcol.ENDC)
        logging.warning('User aborted')
        cleanUpAndQuit("User Aborted")
    except (RuntimeError, TypeError, NameError):
        print(tcol.red, "\nError", tcol.ENDC)
        db.workerMessage(str(worker.id) + ": " + worker.name + " ending with an error")
        logging.critical("Error: %s", sys.exc_info()[0])
        cleanUpAndQuit("Error")
##    finally:
##        quit()

