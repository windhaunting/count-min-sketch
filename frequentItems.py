#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar  1 00:09:43 2018

@author: fubao
"""



'''
Exercise 5 for frequent item
    
''' 

from collections import defaultdict
from operator import itemgetter


'''
Algorithm 1:  use o(k) space, not sketch

Maintain a list of items being counted. Initially the list is empty. For each item, if it is the same
as some item on the list, increment its counter by one. If it differs from all the items on the list,
then if there are less than k items on the list, add the item to the list with its counter set to one.
If there are already k items on the list decrement each of the current counters by one. Delete an
element from the list if its count becomes zero.

'''

import json

def readTwitterDataStrem(fileInput):
    '''
    read data line by line 
    and parse the hashtag as output;  
    use yield iterator
    '''
    
    count = 0
    with open(fileInput) as f:
        
        #from ast import literal_eval
        i = 0
        
        for line in f:
            #print ("line: ", i)
            #line = f.readline() # read only the first tweet/line
    
            #tweet = literal_eval(line)
            try:
                tweet = json.loads(line)         # as Python dict
                #print ("tweet: ", i, tweet["text"])
                
                if "entities" in tweet:
                    hashtags = tweet["entities"]["hashtags"]
                    print("xxxxx: ", hashtags[:15])
                    count += 1
                   
            except:
                #print ("line error: ", i)
                pass 

            i += 1
            
            #if i > 100:
            #    break
        
        
    print ("count: ", count)
    
def frequentItem(element, k):
    '''
    element: hashtag
    find frequent item, occuring at least m/k times  ;   not sketch algorithm
    '''
    
    topkLst = {}  # defaultdict(int)    # top k frequent items 
    
    if element in topkLst:
        topkLst[element] += 1
    
    elif len(topkLst) < k:
        if element not in topkLst:
            topkLst[element] = 1
    else:
        for key in topkLst:
            topkLst[key] -= 1
        
        #delete 
        if topkLst[key] == 0:
            del topkLst[key]
    # topkLst = sorted(topkLst.items(), key=itemgetter(1))      #sort by value
    return topkLst
            
    
def frequentItemCountMinSketch(element, k):
    '''
    element: hashtag
    find frequent item, occuring at least m/k times  ;   count-min sketch algorithm
    '''
    x = 1
    
    m = 0         #total number of elements m arrived so far
    
    #maximum heap
    hp = []
    
    # use count-min sketch
    freq = countMinSketch(m, k)
    if len(hp) < k:
        #push into heap
        heapq.push(hp, (element, freq))
    elif freq > m/k:
        
        

    

twitterFileInputPath = "../tweetstream.txt"
for hashtag in readTwitterDataStrem(twitterFileInputPath):
    print ("output hashtag here: ", hashtag)
    
    