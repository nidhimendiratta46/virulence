import json
import pymongo

# 813286, 21447363, 19697415, 17919972, 783214, 116362700, 23375688, 155659213, 85603854, 50393960, 18839785, 77888423, 128346877, 211862143
collection = pymongo.Connection()['dataset']['tweets']
people = ['813286', '21447363', '19697415', '17919972', '783214', '116362700', '23375688', '155659213', '85603854', '50393960', '18839785', '77888423', '128346877', '211862143','129277744']
tweets = 0
for p in people:
    query = collection.find_one({"id":p})
    if query:
    	tweets+=len(query["tweets"].keys())
    	for tweet in query["tweets"].keys():
    		try :
				print p ,"-", tweet ,"-", len(query["tweets"][tweet]["retweets"].keys())
			except :
				pass
        
print "total number of tweets", tweets