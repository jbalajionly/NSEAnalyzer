#!/usr/bin/python
import sys
import getopt
import os
import csv
import json
import array as arr
import DBConnect
import zipfile
from datetime import datetime
from pymongo import MongoClient
import subprocess
import time
from shutil import move

# Get Command Line Arguments

#def main(argv):
# JSONParser('20190624')

def JSONParser(processdate):

    zip_file = DBConnect.get_zipfile_location("zip")
    print(zip_file)

    for root, dirs, files in os.walk(zip_file):
      for filename in files:
          print(filename)

    #filedate = filename[2:8]
    
    #processdate = datetime.strptime(filedate, '%d%m%y').strftime('%Y%m%d')
    filedate=datetime.strptime(processdate,'%Y%m%d').strftime('%d%m%y')
    filedate1=datetime.strptime(processdate,'%Y%m%d').strftime('%d%b%Y')
    filedate1=str.upper(filedate1)
    print(filedate1)
    unzip_file = DBConnect.get_zipfile_location("unzip")
   # print(unzip_file)

    DBConnect.folder_clear(unzip_file)

    zip_ref = zipfile.ZipFile(os.path.join(zip_file,filename),  'r')
    zip_ref.extractall(unzip_file)
    zip_ref.close()

    json_path = DBConnect.get_zipfile_location("json")
    DBConnect.folder_clear(json_path)
    
    for root, dirs, files in os.walk(unzip_file):
      for filename in files:          
          read_csv(os.path.join(unzip_file,filename), os.path.join(json_path,
                   filename[:-3]+"json"), "pretty", processdate) 
                          

    for root, dirs, files in os.walk(json_path):
      for filename in files:          
          table = DBConnect.get_table(filename.replace(filedate, 'ddmmyy'))
          if(table is None):
             table = DBConnect.get_table(filename.replace(filedate1, 'ddmmyy'))
          DBConnect.delete_table(table, processdate)
          #client = MongoClient('localhost', 27017)
          client = MongoClient("mongodb+srv://Test:Test@mrittiyogan-ycyvf.mongodb.net/NSE?retryWrites=true&w=majority")
          db = client['NSE']
          if table != None:
                uri="mongodb+srv://Test:Test@mrittiyogan-ycyvf.mongodb.net/NSE?retryWrites=true&w=majority"
                collection_currency = db[table]
                print(table)
                p = subprocess.Popen(['C:\\Program Files\\MongoDB\\Server\\4.2\\bin\\mongoimport','--host' ,'MrittiYogan-shard-0/mrittiyogan-shard-00-00-ycyvf.mongodb.net:27017,mrittiyogan-shard-00-01-ycyvf.mongodb.net:27017,mrittiyogan-shard-00-02-ycyvf.mongodb.net:27017','--username','Test','--password','Test','--ssl','--authenticationDatabase','admin','--db', 'NSE', '--collection', table, '--file', json_path+filename])
          else:
                print(table) 
    
    time.sleep(10)   
 
    DBConnect.folder_clear(unzip_file)
    DBConnect.folder_clear(json_path)  

    for root, dirs, files in os.walk(zip_file):
      for filename in files:
        #os.rename(os.path.join(zip_file,filename),os.path.join(DBConnect.get_zipfile_location("archive"),filename))
         move(os.path.join(zip_file,filename),os.path.join(DBConnect.get_zipfile_location("archive"),filename))
 


  #  if os.path.exists(output_file):
   #    os.remove(output_file)
   # else:
   #    print("The file does not exist")

    

# Read CSV File
def read_csv(file, json_file, format,processdate):
    csv_rows = []
    with open(file) as csvfile:
        reader = csv.DictReader(csvfile)
        title = reader.fieldnames       
        
        try:  
          for row in reader:
            try:
              df = row
              df['DATE'] = processdate 
              csv_rows.clear()  
              write_json({key:value for key,value in df.items()}, json_file, format) 
            except:
              print("exception")
        except:
              print("exception")

# Convert csv data into json and write it
def write_json(data, json_file, format):
    with open(json_file, "a") as f:
        if format == "pretty":
            # for x in range(len(data)) :
               f.write(json.dumps(data, sort_keys=False, indent=4, separators=(',', ': '),ensure_ascii=False))
        else:
            # for x in range(len(data)) :
               f.write(json.dumps(data))


