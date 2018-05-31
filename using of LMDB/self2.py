#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri May 25 21:31:27 2018

@author: hujingyi
"""


#%config IPCompleter.greedy=True
import lmdb
import os
import csv
import pickle
from matplotlib import pyplot as plt
import numpy as np #il faut enlever np
#import matplotlib.pyplot as plt, mpld3
import mpld3
from matplotlib import style
from math import sqrt
import progressbar




path_file = "201803_citibikenyc_tripdata.csv"
chunk_size = 100
# getting the DB, creating if missing
lmdb_database = lmdb.open('bike_db', readonly=False, map_size = 2 ** 30)

row = [
        "tripduration","starttime","stoptime","start station id","start station name",
        "start station latitude","start station longitude",
        "end station id","end station name",
        "end station latitude","end station longitude"
        ]

#with open(path_file) as csv_file:
#    reader = csv.reader(csv_file)
with open(path_file, 'rt') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',', quotechar='"')
    next(csv_reader)
    line_number = 1
    with lmdb_database.begin(write = True) as txn:
        rows = progressbar.progressbar(csv_reader, redirect_stdout=True)
        for row in rows :
            notBinaryObject = row[0:11]
            txn.put(pickle.dumps(line_number), pickle.dumps(notBinaryObject)) 
            # key=linenumber, information extracted from the line
            if(line_number == 100000) : # break at row 100000
                break
            #break
        print(line_number)
        
with lmdb_database.begin() as txn: # begin: shortcut for lmdb.transaction
    cursor = txn.cursor()
    plt.rcParams['figure.figsize'] = (16, 9)
    plt.style.use('ggplot')
    data = [] 
    # create a table with the start longitude et latitude used by kmeans
    for key, value in cursor:
        data.append([pickle.loads(value)[6],pickle.loads(value)[7]]) # 10 11
        #data.append([pickle.loads(value)[6],pickle.loads(value)[7],
#                     pickle.loads(value)[10], pickle.loads(value)[11]]) # 10 11
        # only contain the start longitude et latitude and the end?
    print(data[0]) # ['-74.008119', '368']
    
#==============================================================================
#
#                          K MEANS CLUSTERING
#
#==============================================================================

style.use('ggplot')

class K_Means:
    def __init__(self, k = 3, tolerance = 0.0001, max_iterations = 100):
        self.k = k
        self.tolerance = tolerance
        self.max_iterations = max_iterations

    def Euclidean_distance(self, feature_one, feature_two):

        squared_distance = 0
        #Assuming correct input to the function where the lengths of two features are the same

        for i in range(len(feature_one)):
                squared_distance += (feature_one[i] - feature_two[i])**2

        ed = sqrt(squared_distance)
        return ed;

    def fit(self, data):

        self.centroids = {}

        #initialize the centroids, the first 'k' elements in the dataset will be our initial centroids
        for i in range(self.k):
            self.centroids[i] = data[i]

        #begin iterations
        for i in range(self.max_iterations):
            self.classes = {} # tuple: contain many lists
            for i in range(self.k):
                self.classes[i] = []

        #find the distance between the point and cluster; choose the nearest centroid
            for features in data:
                distances = [self.Euclidean_distance(features,self.centroids[centroid]) for centroid in self.centroids]
                classification = distances.index(min(distances))
                self.classes[classification].append(features)

            previous = dict(self.centroids) # stock the last center

            #average the cluster datapoints to re-calculate the centroids
            for classification in self.classes:
                self.centroids[classification] = np.average(self.classes[classification], axis = 0)
                # axis=None, will average over all of the elements of the input array.
                #il faut remplacer np

            isOptimal = True

            for centroid in self.centroids:

                original_centroid = previous[centroid]
                curr = self.centroids[centroid]
                
                #il faut remplacer np
                if np.sum((curr - original_centroid)/original_centroid * 100.0) > self.tolerance:
                    isOptimal = False

            # break out of the main loop if the results are optimal,
            # i.e the centroids don't change their positions much(more than our tolerance)
            if isOptimal:
                break
    def pred(self, data):
        distances = [self.Euclidean_distance(data,self.centroids[centroid]) for centroid in self.centroids]
        classification = distances.index(min(distances))
        return classification

km = K_Means(3) 
dataNumeric = [[float(tuple[0]),float(tuple[1])] for tuple in data]
#dataNumeric = [[float(tuple[0]),float(tuple[1]),float(tuple[2]), float(tuple[3])] for tuple in data]
km.fit(dataNumeric)
print(km)
    
# plotting takes forever becuase of the number of points. However, it's working if you're patient enough.
colors = 10*["r", "g", "c", "b", "k"]

for centroid in km.centroids:
    plt.scatter(km.centroids[centroid][0], km.centroids[centroid][1], s = 130, marker = "x")

    for classification in km.classes:
        color = colors[classification]
        for features in km.classes[classification]:
            plt.scatter(features[0], features[1], color = color,s = 30)
            
    mpld3.show() #launch a web server to view an d3/html figure representation
    
dataNumeric = [[float(tuple[0]),float(tuple[1])] for tuple in data]
#dataNumeric = [[float(tuple[0]),float(tuple[1]),float(tuple[2]), float(tuple[3])] for tuple in data] # start longitude and latitude
dataNumeric 

km.centroids
        
            