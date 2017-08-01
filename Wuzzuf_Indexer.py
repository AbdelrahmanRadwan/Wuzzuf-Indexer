from pyelasticsearch import ElasticSearch
from elasticsearch_dsl import Search
from elasticsearch import helpers, Elasticsearch
from collections import defaultdict
from time import time
import pandas as pd
import requests
import json
import pprint
import csv

Maximum_Number_Of_Records_To_Use = 1000
counter = 0
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
print("Start Reading the User's interests ...")

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

print("Finish mapping the users and their interests...")
#End of Creatring a dictionary/map to map the user to any of the jobs he/she is interested in
#We have a map of [User ID] --> [Job ID]
# ======================================================================================== #
# ======================================================================================== #
print("Start indexing the jobs ...")
# make sure ES is up and running
res = requests.get('http://localhost:9200')
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
Wuzzuf_Job_Posts_Sample = r'DataSet/wuzzuf-job-posts-2014-2016/Wuzzuf_Job_Posts_Sample.csv'
es.indices.create(index='my-index', ignore =400)

#Create a map to hold the user id and his/her Json object
Wuzzuf_Job_Posts_Sample_Map={}

with open(Wuzzuf_Job_Posts_Sample) as f:
    reader = csv.DictReader(f)
    for line in reader:
        #To see the data that will be indexed
        #pprint.pprint(line)
        #print ()
        #The indexing:
        if counter>Maximum_Number_Of_Records_To_Use:
            break
        counter+=1
        Wuzzuf_Job_Posts_Sample_Map[line['id']]=line
        #print to check if the data stored correctly
        #pprint.pprint(Wuzzuf_Job_Posts_Sample_Map[line['id']])
        es.index(index="my-index", doc_type="user", id=counter,body=line)

print("Finish indexing the Wuzzuf_Job_Posts_Sample...")
#Finish indexing the Wuzzuf_Job_Posts_Sample
# ======================================================================================== #
#USerID= raw_input("PLease Enter user's ID, for simple sample, use this '516e4ed'\n")
USerID='846d013c'
JobID = Wuzzuf_Applications_Sample_Map[USerID]
JobID = '516e4ed'
#pprint.pprint(Wuzzuf_Job_Posts_Sample_Map[JobID])

pprint.pprint(es.search(index="my-index", doc_type="user",
          body={'query': {"match": {'city':Wuzzuf_Job_Posts_Sample_Map[JobID]['city']}}}))
'''pprint.pprint(es.search(index="my-index", doc_type="user",
          body={
              'query':
                  {
                    "more_like_this":
                     {
                         'city':Wuzzuf_Job_Posts_Sample_Map[JobID]['city'],
                         'job_title': Wuzzuf_Job_Posts_Sample_Map[JobID]['job_title'],
                         'job_category1': Wuzzuf_Job_Posts_Sample_Map[JobID]['job_category1'],
                         'job_industry1': Wuzzuf_Job_Posts_Sample_Map[JobID]['job_industry1'],
                         'salary_minimum': Wuzzuf_Job_Posts_Sample_Map[JobID]['salary_minimum'],
                         'career_level': Wuzzuf_Job_Posts_Sample_Map[JobID]['career_level'],
                         'experience_years': Wuzzuf_Job_Posts_Sample_Map[JobID]['experience_years'],
                     }
                  }
               }))'''

#Finish using the more like this handler
# ======================================================================================== #