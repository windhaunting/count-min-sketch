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
import heapq
import copy
import math
import hashlib
import csv
import os


'''
Algorithm 1:  use o(k) space, not sketch

Maintain a list of items being counted. Initially the list is empty. For each item, if it is the same
as some item on the list, increment its counter by one. If it differs from all the items on the list,
then if there are less than k items on the list, add the item to the list with its counter set to one.
If there are already k items on the list decrement each of the current counters by one. Delete an
element from the list if its count becomes zero.

'''

import json


class heavyHitter(object):
    
    def __init__(self):
        self.m = 0        # current number of element so far in the streaming
        self.topkLst = {}  # defaultdict(int)    # top k frequent items; not sketching algorithm
        self.hp = []               #answer stored, minheap for heavy hitter problem, count-min skteching
        
    def readTwitterDataStrem(self, fileInput):
        '''
        read data line by line 
        and parse the hashtag as output;  
        use yield iterator
        '''
        
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
                        if hashtags:
                            #print("aaaaa: ", type(hashtags), hashtags)
                            for ht in hashtags:               # multiple hashtags if possible
                                #print("xxxxx: ", type(ht), ht['text'])
                                yield str(ht['text'])          # ht['text'][:15]
                                self.m += 1
                       
                       
                except:
                    #print ("line error: ", i)
                    pass 
    
                #i += 1
                #if i > 100:
                #    break
            
        
    def frequentItem(self, element, k):
        '''
        element: hashtag
        find frequent item, occuring at least m/k times  ;   not sketch algorithm
        '''
                
        if element in self.topkLst:
            self.topkLst[element] += 1
        elif len(self.topkLst) < k-1:  # > k- ;     m/k  not m >=k
            if element not in self.topkLst:
                self.topkLst[element] = 1
            else:
                self.topkLst[element] += 1
        elif element in self.topkLst:
            self.topkLst[element] += 1
        else:
            tmpLst = copy.deepcopy(self.topkLst)
            for key in tmpLst:
                self.topkLst[key] -= 1
            
                #delete 
                if self.topkLst[key] == 0:
                    del self.topkLst[key]
        # topkLst = sorted(topkLst.items(), key=itemgetter(1))      #sort by value
                
    def _hash(self, x, d, w):
        md5 = hashlib.md5(str(hash(str(x).encode('utf-8'))).encode('utf-8'))
        for i in range(d):
            md5.update(str(i).encode('utf-8'))
            yield int(md5.hexdigest(), 16) % w
        
    def countMinSketch(self, element, k):
        '''
        count-min sketch for query each element frequency in the stream
        '''
        # select d hash functions
        delta = 0.01           # delta confidence
        d = math.ceil(math.log(1.0/delta))       # hash function number d
        #print ("d: ", d)
        epsilon = 0.001                           #epsilon
        w = math.ceil(math.e/epsilon)
        sketchTable = [[0]*w for i in range(d)]
        
        # add 
        for table, i in zip(sketchTable, self._hash(element, d, w)):
            table[i] += 1             #add value = 1 here
            
        #query
        return min(table[i] for table, i in zip(sketchTable, self._hash(element, d, w)))
    
            
    def frequentItemCountMinSketch(self, element, k):
        '''
        element: hashtag
        find frequent item, occuring at least m/k times  ;   count-min sketch algorithm
        '''
        
        #m = 0         #total number of elements m arrived so far
        
        #minimum heap
        hp = []
        
        # use count-min sketch
        freq = self.countMinSketch(element, k)
        if len(self.hp) < k-1:            # > m/k  not m >=k
            #push into heap
            heapq.heappush(hp, (element, freq))
        elif freq > self.m/k:
            #check current element is in the heap or not
            flag = False      # flag for in the heap or not 
            for e in self.hp:
                if e[0] == element:
                    #update frequency + 1
                    self.hp[e[0]] += 1
                    flag = True
            if not flag and hp[0] < freq:              #hp[0]'s freq must be <= m/k
                #heapq.heappop(self.hp)
                #heapq.push(self.hp, (element, freq))
                heapq.heappushpop(self.hp, (element, freq))
                
    
    def writeListRowToFileWriterTsv(self, fd, listRow, delimiter):
     #   with open(outFile, "a") as fd:
        writer = csv.writer(fd, delimiter = delimiter, lineterminator='\n')
        writer.writerows([listRow])
    
    
    def executeFrequentItem(self, outFile):
        '''
        no sketching
        '''
        '''
        list =     [3,2,2,5,6,2]  #  [1,1,1,2,2,3]   #  [3,2,2,3,5,2]
        k = 3
        for e in list:
            self.frequentItem(e, k)
            print("result: ", self.topkLst)
        
        '''
        
        k = 500
        twitterFileInputPath = "../tweetstream.txt"
        for hashtag in self.readTwitterDataStrem(twitterFileInputPath):
            #print ("output hashtag here: ", hashtag)
            
            self.frequentItem(hashtag, k)
        
        print ("total number of hashtags: ", self.m)
                
        with open(outFile, "a") as fd:
            #write into file
            self.writeListRowToFileWriterTsv(fd, ["top frequent hashtag",  "Last frequency"], "\t")

            for k, v in self.topkLst.items():
                self.writeListRowToFileWriterTsv(fd, [k, v], "\t")
        fd.close()
        
                        
    def executeHeavyHitter(self, outFile):
        '''
        execute heavy hitter algorithm
        '''
        
        #test element
        '''
        list =     [3,2,2,5,6,2]  #  [1,1,1,2,2,3]   #  [3,2,2,3,5,2]
        k = 3
        m = 0
        for e in list:
            m += 1
            self.frequentItemCountMinSketch(e, m, k)
            print("result: ", self.topkLst)
        '''
        k = 500
        twitterFileInputPath = "../tweetstream.txt"
        for hashtag in self.readTwitterDataStrem(twitterFileInputPath):
            #print ("output hashtag here: ", hashtag)
            
            self.frequentItemCountMinSketch(hashtag, k)
        
        with open(outFile, "a") as fd:
            #write into file
            self.writeListRowToFileWriterTsv(fd, ["top frequent hashtag",  "Last frequency"], "\t")
            for k, v in self.topkLst.items():
                self.writeListRowToFileWriterTsv(fd, [k, v], "\t")
        fd.close()


heavyHitterObj = heavyHitter()

outFile1 = "ouputHashTag1_NoSketch.tsv"
os.remove(outFile1) if os.path.exists(outFile1) else None
heavyHitterObj.executeFrequentItem(outFile1)

outFile2 = "ouputHashTag2_CountMinSketch.tsv"
os.remove(outFile2) if os.path.exists(outFile2) else None
heavyHitterObj.executeHeavyHitter(outFile2)

