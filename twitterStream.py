import tweepy
import json
import pickle
import os
import pymongo
import ssl

consumerKey = "A1pZOTrTqVmePalcUGi2ZA"
consumerSecret = "YD72Srbim8ezEwVFVl52GmS2Ws5Tvtcxp5TcgfhLS0Y"
accessToken = "129277744-2GsOUIUNTLw7MldL64BSjOf9NDn9HRSr9TxY0poU"
accessTokenSecret = "hD2puf6vMFv0ZX8legYGCN3eUfDLW95b9bblkPYU0"
f = open("english","r")
stopwords = f.read().split("\n")

# 813286, 21447363, 19697415, 17919972, 783214, 116362700, 23375688, 155659213, 85603854, 50393960, 18839785, 77888423, 128346877, 211862143
collection = pymongo.Connection()['dataset']['tweets']

def hasLink(str):
    s='http://t.co'
    pos=str.find(s)
    if pos != -1:
        return True
    else:
        return False

def FilterStatus(textword):
    wordList = textword.split(" ")
    newList = []
    for i in wordList:
        if i not in stopwords:
            newList.append(i)
    return ' '.join(newList) 
    

class StdOutListener(tweepy.streaming.StreamListener):
    ''' Handles data received from the stream. '''
    Authors = ['813286', '21447363', '19697415', '17919972', '783214', '116362700', '23375688', '155659213', '85603854', '50393960', '18839785', '77888423', '128346877', '211862143','129277744']
    def on_status(self, status):
        # Prints the text of the tweet
        try :
            if status.__dict__.has_key("retweeted_status") and collection.find_one({"id":status.retweeted_status.user.id_str}):
                print "1-ReTweet",status.text
                query = collection.find_one({"id":status.retweeted_status.user.id_str})
                try :
                    query["tweets"][status.retweeted_status.id_str]["retweets"][status.id_str] = {
                                                                                            "time" : status.created_at,
                                                                                            "location" : status.user.location,
                                                                                            "verified" : status.user.verified,
                                                                                            "username" : status.user.screen_name,
                                                                                            "description" : status.user.description
                                                                                            }
                except TypeError:
                    pass                
                collection.save(query)

            elif status.user.id_str in self.Authors and status.in_reply_to_status_id_str == None and status.__dict__.has_key("retweeted_status")==False:
                print "2-Original", status.text
                query = collection.find_one({"id":status.user.id_str})
                if not query :
                    collection.insert( {
                                            "id" : status.user.id_str,
                                            "username" : status.user.screen_name,
                                            "created_time" : status.user.created_at,
                                            "description" : status.user.description,
                                            "location" : status.user.location,
                                            "timezone" : status.user.time_zone,
                                            "utc_offset" : status.user.utc_offset,
                                            "verified" : status.user.verified,
                                            "tweets" : {
                                                        status.id_str : 
                                                            {
                                                            "text" : FilterStatus(status.text),
                                                            "length" : len(status.text),
                                                            "created_at" : status.created_at,
                                                            "#" : status.entities["hashtags"],
                                                            "followers_count" : status.user.followers_count,
                                                            "link" : hasLink(status.text),
                                                            "retweets" : {},
                                                            }
                                                       }
                                        }
                                     )
                else :
                    query["tweets"][status.id_str] = {                               
                                                    "text" : FilterStatus(status.text),
                                                    "length" : len(status.text),
                                                    "created_at" : status.created_at,
                                                    "#" : status.entities["hashtags"],
                                                    "followers_count" : status.user.followers_count,
                                                    "link" : hasLink(status.text),
                                                    "retweets" : {},
                                                    }
                    collection.save(query)
        except KeyError:
            pass
        return True
 
    def on_error(self, status_code):
        print('Got an error with status code: ' + str(status_code))
        return True # To continue listening
 
    def on_timeout(self):
        print('Timeout...')
        return True # To continue listening
    
 

if __name__ == '__main__':
    listener = StdOutListener()
    auth = tweepy.OAuthHandler(consumerKey, consumerSecret)
    auth.set_access_token(accessToken, accessTokenSecret)
    stream = tweepy.streaming.Stream(auth, listener, timeout=216000)
    try :
        stream.filter(follow=['813286', '21447363', '19697415', '17919972', '783214', '116362700', '23375688', '155659213', '85603854', '50393960', '18839785', '77888423', '128346877', '211862143','129277744'])
    except ssl.SSLError:
        print "Bye !"
