__author__ = 'tracy'


from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import re
import sys
import json
import dateutil.parser
from pytz import timezone
import pytz
import tweepy
import time
import urllib3
import csv

#Listener Class Override
class listener(StreamListener):

      def __init__(self, start_time, time_limit, keywords):

          self.time = start_time
          self.limit = time_limit
          self.lastID = None
          self.regex = re.compile('|'.join(keywords).lower())


      def on_data(self, data):
         # print time.time() - self.time
          while (time.time() - self.time) < self.limit:
              try:
                  tweet = json.loads(data)
                  print tweet['id']

                  if not tweet.has_key('id_str'):
                     #print 'No tweet ID - ignoring tweet.'
                     return True

                  #### ignore duplicates tweets
                  tweetID = tweet['id_str']
                  if tweetID != self.lastID:
                     self.lastID = tweet['id_str']
                  else:
                     return True

                  if not tweet.has_key('user'):
                     #print 'No user data - ignoring tweet.'
                     return True
                  user = tweet['user']['name']
                  text = parse_text(tweet['text'])
                  # print text


                  ### mathces the key words
                  # matches = re.search(self.regex, text.lower())
                  # if not matches:
                  #     return True

                  #### remove the retweets
                  # if tweet['retweeted'] or 'RT @'  in tweet['text']:
                  #     return True

                  location = tweet['user']['location']
                  source = tweet['source']

                  d = dateutil.parser.parse(tweet['created_at'])
                  d_tz = pytz.timezone('UTC').normalize(d)
                  localtime = d.astimezone(timezone('US/Pacific'))
                  tmstr = localtime.strftime("%Y%m%d-%H:%M:%S")
                  #print tweetID, text

                  # saveFile = open('raw_tweets.json', 'a')
                  # saveFile.write(data)
                  # saveFile.write('\n')
                  # saveFile.close()

                  # append the hourly tweet file
                  with open('tweets-%s.data' % tmstr.split(':')[0], 'a+') as f:
                       f.write(data)

                  geo = tweet['geo']
                  if geo and geo['type'] == 'Point':
                     coords = geo['coordinates']
                  else:
                      return True

                  # with open('mydata.txt', 'a+') as f:
                  #      #f.write('tweetID,creat_time,Coord1,Coord2,Text')
                  #      print("%s,%s,%f,%f,%s" % (tweetID,tmstr,coords[0],coords[1],text))
                  #      f.write("%s,%s,%f,%f,%s\n" % (tweetID,tmstr,coords[0],coords[1],text))

              except BaseException, e:
                  print 'failed ondata,', str(e)
                  time.sleep(5)
                  pass
              except urllib3.exceptions.ReadTimeoutError, e:
                  print 'failed connection,', str(e)
                  time.sleep(5)
                  pass

          exit()

      def on_error(self, status):
          print status

def parse_text(text):
    """
    Read an txt file
    Replace numbers, punctuation, tab, carriage return, newline with space
    Normalize w in wordlist to lowercase
    """
    text = text.encode('latin1', errors='ignore')
    text = text.rstrip('\n')
    # print text
    #text = text.replace('\n', ' ')
    # wordlist = text.split(" ")
    # stopWords = ['a', 'able', 'about', 'across', 'after', 'all', 'almost', 'also',
    #              'am', 'among', 'an', 'and', 'any', 'are', 'as', 'at', 'be',
    #              'because', 'been', 'but', 'by', 'can', 'cannot', 'could', 'dear',
    #              'did', 'do', 'does', 'either', 'else', 'ever', 'every', 'for',
    #              'from', 'get', 'got', 'had', 'has', 'have', 'he', 'her', 'hers',
    #              'him', 'his', 'how', 'however', 'i', 'if', 'in', 'into', 'is',
    #              'it', 'its', 'just', 'least', 'let', 'like', 'likely', 'may',
    #              'me', 'might', 'most', 'must', 'my', 'neither', 'no', 'nor',
    #              'not', 'of', 'off', 'often', 'on', 'only', 'or', 'other', 'our',
    #              'own', 'rather', 'said', 'say', 'says', 'she', 'should', 'since',
    #              'so', 'some', 'than', 'that', 'the', 'their', 'them', 'then',
    #              'there', 'these', 'they', 'this', 'tis', 'to', 'too', 'twas', 'us',
    #              've', 'wants', 'was', 'we', 'were', 'what', 'when', 'where', 'which',
    #              'while', 'who', 'whom', 'why', 'will', 'with', 'would', 'yet',
    #              'you', 'your']
    # #wordlist = [w for w in wordlist if (w not in stopWords)]
    # wordlist = [w for w in wordlist if (len(w) >= 3 and w not in stopWords)]
    # return " ".join(str(w) for w in wordlist)
    return text



def read_csv(file):
    data = []
    with open(file) as f:
        for line in f:
            data.append(line.strip('\n'))
    return data

def OAuth(consumer_key, consumer_secret, access_token, access_secret):
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    try:
        redirect_url = auth.get_authorization_url()
        print redirect_url
    except tweepy.TweepError:
        print 'Error! Failed to get request token.'
    api = tweepy.API(auth)
    print api.me().name
    return auth


def main():
    consumer_key = '0pw1FQTetWrPExm2LJ1Ygg1kf'
    consumer_secret = 't2GQcNiyXr6yVNi3c5QA0g5TywF1jyAdKNYmE1QrxHMMDgcjW3'
    access_token = '3332923519-LnlDeEhjj0LhnkCbn3p8kr4CCLb9BBf4tXJbGne'
    access_secret = 'GVlI35yuGp5nBTpgf0if2MS73qIFu8AJHJPtGvK3ZTIHt'
    auth = OAuth(consumer_key, consumer_secret, access_token, access_secret)


    start_time = time.time() #grabs the system time
    time_limit = 10000
    keywords = read_csv('candidates.txt')

    # print keywords
    #keywords = ["happy", "sad"]
    #print '|'.join(keywords).lower()
    #keyword_list = ['obama'] #track list

    twitterStream = Stream(auth, listener(start_time, time_limit, keywords))
    twitterStream.filter(track= keywords, languages=['en'])

if __name__ == '__main__':
    main()
