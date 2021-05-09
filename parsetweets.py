#!/usr/bin/env python
"""parse twitter feed for covid 19 information 
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import getopt
import re
import sys
from collections import defaultdict
import pandas as pd
import sys
import argparse
from argparse import RawTextHelpFormatter
import os
from os.path import abspath
from os import listdir
from os.path import isfile, join
from numpy import arange
from datetime import datetime as dt


def findurls(str):
    urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\), ]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', str)
    return list(map(lambda x: x.strip(), urls))


def findbloodgroup(str):
    if re.search("AB\s*[+-]", str):
        bloodgroup = re.findall("AB\s*[+-]", str)
    elif re.search("[ABO]\s+[+-]", str):
        bloodgroup = re.findall("[ABO]\s*[+-]", str)
    else:
        bloodgroup = "NA"
    return (bloodgroup)


def findContact(str):
    contacts = re.findall("[91]*\s*[0-9]{2}\s*[0-9]{4}\s*[0-9]{4}",str)
    if (len(contacts)) == 0:
        contacts = re.findall("[91]*\s*[0-9]{5}\s*[0-9]{5}",str)
    return list(map(lambda x: x.strip(), contacts))


def findCity(str):
    cities = get_all_cities()
    tweetcities = []
    for city in cities:
        if city.upper() in str.upper():
            tweetcities.append(city)
    return tweetcities


def get_all_cities():
    return ["Hyderabad", "Bangalore", "Mumbai", "Bombay", "Delhi", "Thane", "Nagpur", "Chennai", "Nashik", "Waranagal"]


def resourcereq(user, tweet_date, tweet, id):
    # This proc parses tweets from request hashtags and returns username,resource,contact,bloodtype,city,fulltweet
    # Looking for plasma and oxygen
    # if Plasma, additionally look for blood type
    restricted_words = ['MODI', 'GOVERNMENT', 'BJP', 'CONGRESS', 'RESIGN']
    if not (any(restricted_word in tweet.upper() for restricted_word in restricted_words)):
        if "AVAIL" in tweet.upper():
            req = "AVAILABLE"
        elif any(need_word in tweet.upper() for need_word in ["NEED", "WANT", "REQUIRE"]):
            req = "NEED"
        else:
            req = "UNKNOWN"
        username = user
        if "OXYGEN" in tweet.upper():
            resource = "oxygen"
        elif "PLASMA" in tweet.upper():
            resource = "plasma"
        elif "ICU" in tweet.upper():
            resource = "ICU Bed"
        else:
            resource = "other"
        bloodgroup = findbloodgroup(tweet)
        contact = findContact(tweet)
        cities = findCity(tweet)
        urls = findurls(tweet)
        tweet_url = "https://twitter.com/twitter/statuses/{}".format(id)
        # print("username = {}, resource = {}, Purpose = {},  bloodgroup = {}, contact = {}, cities = {} ".format(username,resource,req,bloodgroup,contact,cities,tweet))
        return [username, tweet_date, resource, req, bloodgroup, contact, urls, cities, tweet, tweet_url]


def donationtweets(tweet):
    # This proc parses tweets from potential donation hastags and returns username,donation links data
    # Assume tweet is of form username,text,hashtag
    # Ignore any tweets containing restricted strings
    donation_owner = defaultdict(str)
    restricted_words = ['modi', 'government', 'BJP', 'congress', 'resign']
    if not (any(restricted_word.upper() in tweet.upper() for restricted_word in restricted_words)):
        urls = findurls(tweet)
        if len(urls) > 0:
            #     print("{},{}".format(tweet.split(',')[0],urls))
            return (tweet.split(',')[0], urls)


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description='Code extracts information relevant to COVID 19 from tweets')
    parser.add_argument('--csv', help='csv', required=True, dest='csv')
    input_args = vars(parser.parse_args())

    # Define a pandas dataframe to store the date:
    processed_tweets = pd.DataFrame(
        columns=['username', 'tweet_date', 'resource', 'req', 'bloodgroup', 'contact', 'urls', 'cities', 'tweet',
                 'tweeturl'])
    raw_tweets = pd.read_csv(input_args['csv'])
    raw_tweets = raw_tweets.drop_duplicates('text')

    for index, row in raw_tweets.iterrows():
        # Testing resource request function
        processed_tweet = resourcereq(row['username'], row['tweet_date'], row['text'], row['id'])
        if processed_tweet: # Empty check. TODO: Why are these empty in the first place.
            processed_tweets.loc[len(processed_tweets)] = processed_tweet

        # Put progress onto console
        if (len(processed_tweets) % 100 == 0):
            print(f"Processed {len(processed_tweets)} tweets thus far")

    # Define working path and filename
    path = os.getcwd()
    filename = input_args['csv'].replace("raw_tweets", "parsed_deduped_data")
    processed_tweets.to_csv(filename, index=False)
    
    with pd.ExcelWriter(filename.replace('csv', 'xlsx')) as writer:
        processed_tweets.to_excel(writer, sheet_name='India')

if __name__ == '__main__':
    main()
