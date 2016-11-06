#!/usr/bin/python3
import sys
import imgur_dl

try:
    imgur_dl.worker.id = int(sys.argv[1])
    imgur_dl.worker.name = str(sys.argv[2])
    imgur_dl.numOfPics = int(sys.argv[3])
    imgur_dl.minSize = int(sys.argv[4])
    imgur_dl.delayTime = float(sys.argv[5])
except:
    print("Arguments error")
    print("imgur_opt.py [1: worker id] [2: worker name] [3: number of pics] [4: min size in bytes] [5: delay time]")
    quit()
    

##imgur_dl.numOfPics = int(input(' Number of images to download [default 10]: ') or 10)
##imgur_dl.minSize = int(input(' Minimum file size (in bytes) [default 64000]: ') or 64000)
##imgur_dl.delayTime = float(input(' Rate limiter delay [default 0.3]: ') or 0.3)
imgur_dl.taskType = "user input"
imgur_dl.downloadLoop()
