import mysql.connector
from mysql.connector import errorcode
import datetime
import sys
from time import sleep

import worker_info
worker = worker_info.worker
print("Worker ID: " + str(worker.id) + ", Worker name: " + worker.name)



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

def formatStatus(input):
    if input == "Finished":
        return tcol.cyan
    elif input == "Downloading":
        return tcol.green
    else:
        return tcol.yellow

def parseFileDetails(isDupe, dupeOf, isRemoved):
    if isRemoved == 0:
        if isDupe == 0:
            return str(tcol.cyan + "New File" + tcol.ENDC)
        else:
            return str(tcol.yellow + "Duplicate of: " + tcol.cyan + dupeOf + tcol.ENDC)
    elif isRemoved == 1:
        return str(tcol.red + "Removed" + tcol.ENDC)
    else:
        return "Unknown parameters"
    
while True:
    try:
        drawtxt = []
        
        # =====================
        # Worker Table
        #======================

        cnx = mysql.connector.connect(**worker.dbconfig)
        cursor = cnx.cursor()

        query = ("SELECT worker_id, worker_name, worker_status, worker_target, worker_progress, last_updated, r, m, i FROM workers ORDER BY last_updated DESC")

        cursor.execute(query)
        drawtxt.append("\033c")
        for (worker_id, worker_name, worker_status, worker_target, worker_progress, last_updated, r, m, i) in cursor:
            drawtxt.append("{} {}: {}\t[ {} of {} ] [R: {}] [M: {}] [I: {}]\t{}".format((formatStatus(worker_status) + worker_status.ljust(15) + tcol.ENDC), worker_id, worker_name.ljust(10), str(worker_progress).rjust(4) , str(worker_target).rjust(4), str(r).rjust(4), str(m).rjust(4), str(i).rjust(4), tcol.cyan + str(last_updated)))
 #       cnx.close()

        # =====================
        # Messages Table
        #======================

        drawtxt.append("\n")
        cnx = mysql.connector.connect(**worker.dbconfig)
        cursor = cnx.cursor()
        query3 = ("select id, ts, msg from messages ORDER BY id DESC LIMIT 0, 10")
        cursor.execute(query3)
        for (id, ts, msg) in cursor: 
            drawtxt.append(str(tcol.cyan + str(ts) + ": " + tcol.white + msg ))
            #print("{} : {} {} by {}  @ {}".format(str(id).rjust(15), hash_val, filename, first_seen, str(first_seen_by).ljust(10))
 #       cnx.close()
        
        # =====================
        # Hash Table
        #======================
        
        drawtxt.append("\n")
        cnx = mysql.connector.connect(**worker.dbconfig)
        cursor = cnx.cursor()
        query2 = ("SELECT id, hash_val, filename, first_seen, first_seen_by FROM hash_table ORDER BY id DESC LIMIT 0, 5")
        cursor.execute(query2)
        for (id, hash_val, filename, first_seen, first_seen_by) in cursor: 
            drawtxt.append(str(tcol.yellow + first_seen_by.ljust(10) + " " + tcol.cyan + filename + " " + tcol.blue + str(id).rjust(8) + tcol.white + " " + hash_val + " " + tcol.cyan + str(first_seen)))
            #print("{} : {} {} by {}  @ {}".format(str(id).rjust(15), hash_val, filename, first_seen, str(first_seen_by).ljust(10))
 #       cnx.close()

        # =====================
        # Seen Files Table
        #======================

        drawtxt.append("\n")
        cnx = mysql.connector.connect(**worker.dbconfig)
        cursor = cnx.cursor()
        query2 = ("select id, file_name, isDupe, dupeOf, firstSeen, seenBy, fileSize, isRemoved from seen_files ORDER BY id DESC LIMIT 0, 25")
        cursor.execute(query2)
        for (id, file_name, isDupe, dupeOf, firstSeen, seenBy, fileSize, isRemoved) in cursor: 
            drawtxt.append(str(tcol.yellow + seenBy.ljust(10) + " " + tcol.cyan + file_name + " " + tcol.blue + str(id).rjust(8) + tcol.green + str(fileSize / 1000).rjust(9) + " Kb " + " " + parseFileDetails(isDupe, dupeOf, isRemoved)))
            #print("{} : {} {} by {}  @ {}".format(str(id).rjust(15), hash_val, filename, first_seen, str(first_seen_by).ljust(10))
        cnx.close()



        
        print(*drawtxt, sep='\n')
        #print(drawtxt)
        sleep(1)
                
    except mysql.connector.Error as err:
        print("\n\nAborting, Closing DB")
        cnx.close()
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    except KeyboardInterrupt:
        print(tcol.red, "\n\n\n\n\n\n\n\nInterrupt received, stopping", tcol.ENDC)
        print("Closing DB")
        quit()
    finally:
        #print("Closing DB")
        cnx.close()

    
