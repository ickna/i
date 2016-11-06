#!/usr/bin/python3

import imgur_dl
import sys

try:
    imgur_dl.worker.id = int(sys.argv[1])
    imgur_dl.worker.name = str(sys.argv[2])

except:
    print("Arguments error")
    print("imgur_opt.py [1: worker id] [2: worker name]")
    quit()

imgur_dl.numOfPics = 20
imgur_dl.minSize = 100000
imgur_dl.delayTime = 0.1
imgur_dl.taskType = "cron task"


imgur_dl.downloadLoop()
