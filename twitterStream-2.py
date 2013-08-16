import tweepy
import os
import ssl

consumerKey = "A1pZOTrTqVmePalcUGi2ZA"
consumerSecret = "YD72Srbim8ezEwVFVl52GmS2Ws5Tvtcxp5TcgfhLS0Y"
accessToken = "129277744-2GsOUIUNTLw7MldL64BSjOf9NDn9HRSr9TxY0poU"
accessTokenSecret = "hD2puf6vMFv0ZX8legYGCN3eUfDLW95b9bblkPYU0"
f = open("english","r")
stopwords = f.read().split("\n")
f.close()
# 813286, 21447363, 19697415, 17919972, 783214, 116362700, 23375688, 155659213, 85603854, 50393960, 18839785, 77888423, 128346877, 211862143

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
    Authors = ['16303106', '5402612', '160817572', '62513246', '561684253', '558797310','52551600', '169686021', '115485051', '20609518', '250831586', '23832022', '130734452', '36686415', '130734452', '345811633', '17560096', '22940219','813286', '21447363', '19697415', '17919972', '783214', '116362700', '23375688', '155659213', '85603854', '50393960', '18839785', '77888423', '128346877', '211862143','129277744']
    def on_status(self, status):
        # Prints the text of the tweet
        try :
            if status.__dict__.has_key("retweeted_status") :
                if os.path.isfile(status.retweeted_status.user.id_str + "/" + status.retweeted_status.id_str):
                    f = open(status.retweeted_status.user.id_str + "/" + status.retweeted_status.id_str,"a")
                    string = status.user.id_str + " ||| " + str(status.created_at)+ " ||| "
                    if status.user.location :
                        string += status.user.location.encode('ascii','ignore')
                    string += " ||| " + str(status.user.verified) + " ||| " + status.user.screen_name.encode('ascii','ignore') + " ||| " 
                    if status.user.description :
                        string += status.user.description.encode('ascii','ignore')
                    string += "\n"
                    f.write(string)
                    f.close()
            elif status.user.id_str in self.Authors and status.in_reply_to_status_id_str == None and status.__dict__.has_key("retweeted_status")==False:
                print "2-Original", status.text.encode('ascii', 'ignore')
                d = os.path.dirname(status.user.id_str+"/"+status.id_str)
                if not os.path.exists(d) :
                    os.makedirs(d)
                    f = open(status.user.id_str+"/"+"user.info","w")
                    string = status.user.screen_name.encode('ascii', 'ignore') + "\n"  
                    if status.user.description :
                        string += status.user.description.encode('ascii', 'ignore')
                    string += "\n"
                    if status.user.location :
                        string += status.user.location.encode('ascii', 'ignore') 
                    string += "\n" + str(status.user.time_zone) + "\n" + str(status.user.created_at) + "\n" + str(status.user.utc_offset) + "\n" + str(status.user.verified)
                    f.write(string)
                    f.close()
                f = open(status.user.id_str+"/"+status.id_str,"w")
                string = FilterStatus(status.text.encode('ascii', 'ignore')) + " ||| " + str(len(status.text.encode('ascii', 'ignore'))) + " ||| " + str(status.created_at) + " ||| " + str(status.entities["hashtags"]) + " ||| " + str(status.user.followers_count) +" ||| "+ str(hasLink(status.text.encode('ascii','ignore'))) + "\n"
                f.write(string)
                f.close()
        except AttributeError:
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
    stream = tweepy.streaming.Stream(auth, listener)
    try :
        stream.filter(follow=listener.Authors)
    except ssl.SSLError:
        print "Bye !"