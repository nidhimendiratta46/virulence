import os

count = 0
countFiles = 0

for dirname, dirnames, filenames in os.walk('.'):
    # print path to all subdirectories first.
    for subdirname in dirnames:
        files = os.walk("./"+subdirname).next()[2]
        print subdirname
        #print files
        files.remove("user.info")    
        for i in files:
            countFiles += 1
            f = open("./"+subdirname+"/"+i,"r")
            line = f.readline()     
            tweetList = line.split(" ||| ")
            while len(tweetList) < 4:
                tweetList = f.readline().split(" ||| ")
            retweetCount = 0
            for line in iter(f):
                tweetList = line.split(" ||| ")
                if len(tweetList) > 4 :
                    retweetCount += 1
            if retweetCount > 1000:
                count+=1
            #print i, retweetCount
print "Viral tweet : ", count, "Total Tweets :", countFiles
