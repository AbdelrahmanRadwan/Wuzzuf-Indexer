from pyelasticsearch import ElasticSearch
from elasticsearch import helpers, Elasticsearch
from collections import defaultdict
from time import time
import pandas as pd
import requests
import json
import pprint
import csv

# ======================================================================================== #
## Create a map between the user id and any of his/her jobs id
# ======================================================================================== #
# each value in each column is appended to a list
columns = defaultdict(list)
#The path of the applications file
Wuzzuf_Applications_Sample = r'DataSet/wuzzuf-job-posts-2014-2016/Wuzzuf_Applications_Sample.csv'
#Another path for testing the dataset and the code (smaller dataset(only 10 records) )
#Wuzzuf_Applications_Sample = r'DataSet/wuzzuf-job-posts-2014-2016/Test.csv'
#Read the dataset and manage it into columns
with open(Wuzzuf_Applications_Sample) as f:
    reader = csv.DictReader(f) # read rows into a dictionary format
    for row in reader: # read a row as {column1: value1, column2: value2,...}
        for (k,v) in row.items(): # go over each column name and value
            columns[k].append(v) # append the value into the appropriate list
                                 # based on column name k
#Create a map to hold the user id and his/her job id
Wuzzuf_Applications_Sample_Map={}
for i in range(0,len(columns['user_id'])):
    Wuzzuf_Applications_Sample_Map[columns['user_id'][i]]=columns['job_id'][i]
#print the map if needed:
#pprint.pprint(Wuzzuf_Applications_Sample_Map)

#End of Creatring a dictionary/map to map the user to any of the jobs he/she is interested in
#We have a map of [User ID] --> [Job ID]
# ======================================================================================== #

# make sure ES is up and running
res = requests.get('http://localhost:9200')
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
Wuzzuf_Job_Posts_Sample = r'DataSet/wuzzuf-job-posts-2014-2016/Wuzzuf_Job_Posts_Sample.csv'
es.indices.create(index='my-index', ignore =400)
with open(Wuzzuf_Job_Posts_Sample) as f:
    reader = csv.DictReader(f)
    for line in reader:
        #To see the data that will be indexed
        #pprint.pprint(line)
        #print ()
        #The indexing:
        es.index(index="my-index", doc_type="user",body=line)

