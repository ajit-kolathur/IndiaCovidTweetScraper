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
import numpy as np
import matplotlib.pyplot as plt
import sys
import argparse
from argparse import RawTextHelpFormatter
import os
from os.path import abspath
from os import listdir
from os.path import isfile, join
from numpy import arange


def findurls(str):
    urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\), ]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', str)
    return (urls)


def findbloodgroup(str):
    if re.search("AB\s*[+-]", str):
        bloodgroup = re.findall("AB\s*[+-]", str)
    elif re.search("[ABO]\s+[+-]", str):
        bloodgroup = re.findall("[ABO]\s*[+-]", str)
    else:
        bloodgroup = "NA"
    return (bloodgroup)


def findContact(str):
    contact = re.findall("[91]*\s*[0-9]{2}\s*[0-9]{4}\s*[0-9]{4}", str)
    return (contact)


def findCity(str):
    cities = ["Hyderabad", "Bangalore", "Mumbai", "Bombay", "Delhi", "Thane", "Nagpur", "Chennai", "Nashik"]
    tweetcities = []
    for city in cities:
        if city.upper() in str.upper():
            tweetcities.append(city)
    return (tweetcities)


def resourcereq(tweet):
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
        username = tweet.split(',')[0]
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
        # print("username = {}, resource = {}, Purpose = {},  bloodgroup = {}, contact = {}, cities = {} ".format(username,resource,req,bloodgroup,contact,cities,tweet))
        return (username, resource, req, bloodgroup, contact, cities, tweet)


def donationtweets(tweet):
    # This proc parses tweets from potential donation hastags and returns username,donation links data
    # Assume tweet is of form username,text,hashtag
    # Ignore any tweets containing restricted strings
    donation_owner = defaultdict(str)
    restricted_words = ['modi', 'government', 'BJP', 'congress', 'resign']
    if not (any(restricted_word.upper() in tweet.upper() for restricted_word in restricted_words)):
        urls = findurls(tweet)
        if len(urls) > 0:
            # 	print("{},{}".format(tweet.split(',')[0],urls))
            return (tweet.split(',')[0], urls)


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description='Code extracts information relevant to COVID 19 from tweets')
    parser.add_argument('--csv', help='csv', required=True, dest='csv')
    input_args = vars(parser.parse_args())
    with open(input_args['csv']) as csvfile:
        # for line in csvfile:
        #     line = line.strip()
        for x in range(10):
            line = next(csvfile).strip()
            # Testing resource request function
            resourcereq(line)
            line = csvfile.readline()


if __name__ == '__main__':
    main()
