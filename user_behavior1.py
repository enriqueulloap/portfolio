#!/usr/bin/env python
# coding: utf-8

# # User Behavior in an Educational Social Network (Part 1)

# 
# A startup has created a platform where students can find and share learning material for their studies. They collect events from users as they browse the platform.
# 
# They provided us a sample dataset with navigations events splitted on two files with the name **part-[0000x].json**.
# 
# Our mission is to extract information that allows them to analyze what the users are accessing and how they do that.

# ## Requeriments

# * A MongoDB server running on localhost with an user 'root' on the admin database and the roles "root", "userAdminAnyDatabase" and "readWriteAnyDatabase"
# * MongoDB Compass
# * Jupyter Notebook `pip install notebook`
# * PyMongo Python module `pip install pymongo`

# ## Context Analysis

# ### Data Exploration

# First, we explore the content of the JSON files to determine the best approach for this case. Each file contains one JSON object per line and they are not separated by commas.

# ![Json object sample](img/json_file1.png)

# Also, we noticed that not all the objects have the same keys, here is one case that has 3 keys relative to a marketing campaign:

# ![Json object sample](img/json_file2.png)

# So, how we have data without a defined structure, we decided to load into a MongoDB database through a data pipeline.

# ## JSON to MongoDB Data Pipeline

# 
# ### Base Functions
# 

# 
# #### JSON file to List of Dictionaries

# Function that process a JSON file line by line and append each object to a Python array, returning a list of dictionaries:

# In[38]:


# json files containing one dict per line, it is not a list (comma separated) of dicts

from bson import json_util

def read_json(file):
    """Convert JSON to Python objects, which PyMongo will then convert to BSON for sending to MongoDB"""

    dict_list = []
    
    # reading the JSON data using json_util.loads()
    for line in open(file, "r"):
        dict_list.append(json_util.loads(line))

    return dict_list
    


# 
# #### List of Dictionaries to MongoDB Collection

# Function for make a connection to a MongoDB server and insert JSON objects with the insert_many() method.

# In[2]:


from pymongo import MongoClient

def _connect_mongo(host, port, username, password, db):
    """ A util for making a connection to mongo """

    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s?authSource=admin' % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)


    return conn[db]


def write_mongo(db, collection, dict_list, host='localhost', port=27017, username=None, password=None):
    """ Read from dict list and Store into MongoDB collection """

    # Connect to MongoDB
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)
    
    # !! Delete collection if exists
    try:
        db[collection].drop()
        
    except:
        pass

    
    # Insert to the specific DB and Collection
    
    return  db[collection].insert_many(dict_list)


# ### Data Pipeline Functions

# #### Extract

# The Extract function will go through the folder and execute read_json() for each file with the pattern **part-*.json**

# In[11]:


# we extract all json objects from the files that match the indicated pattern
# the function returns an array, thus, we concatenate the returned array to an empty array

import glob

def extract():

    events = []
    
    for file in glob.glob("datasets/part-*.json"):
        
        # concatenate arrays (append() will create an array of arrays...)
        events = events + read_json(file)
    
    return events


# 
# #### Load
# 

# The Load function calls write_mongo() with the conection parameters to our MongoDB server and the names of the database and collection that we will create (EducationalPlatform and events). It receives the List of Dictionaries to insert in the database as argument.

# In[8]:


def load(events):

    return write_mongo('EducationalPlatform', 'events', events, 'localhost', 27017, 'root', 'root')
    


# ### Data Pipeline Definition

# The Pipeline calls the Extract and Load functions

# In[12]:


def etl():
    
    extracted_data = extract()
    
    loaded_data = load(extracted_data)
    
    return loaded_data


# 
# ### Main Function Definition
# 

# The main function will execute the Data Pipeline if we run this notebook from a terminal

# In[ ]:


if __name__ == "__main__":
    
    etl()


# ## Data Pipeline Execution

# For execution we can convert this notebook to a Python script with the following command (from the same folder where the notebook is):

# `jupyter nbconvert --to script Users\ behavior\ of\ an\ educational\ social\ network\ \(Part\ 1\).ipynb`

# And then we execute the Python script with our Data Pipeline:

# In[ ]:





# And we have loaded our data into MongoDB:

# ![mongodb collection 1](img/mongodb_collection1.png)

# ![mongodb collection 2](img/mongodb_collection2.png)

# Now we can analyze the data to decide which fields are relevants to our objective.
