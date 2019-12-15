import pip 
from pymongo import MongoClient
import DBConnect
import Parse
import os
import zipfile
import CSVtoJSON
import requests
import urllib3
import wget

while(DBConnect.getBatchDate()):  

 Date=DBConnect.getBatchDate()
 print(Date)
 #DBConnect.UpdateBatchDate(Date)
#form URL
 listurl=DBConnect.getRequestURL()
 
 parsedURL=[]

 if (len(listurl)>1):
   for y in listurl:
     parsedURL.append(Parse.parseURL(y,Date))
   
   Zipfile=DBConnect.get_zipfile_location("zip")

   for url in parsedURL:  
         print(url)    
         r = requests.get(url, timeout=250)   
        # r=urllib.request.urlopen(url)      
         head,tail=os.path.split(url)
         Zipfile_d=os.path.join(Zipfile,tail)
         print(tail)      
         print(Zipfile_d)         
         with open(Zipfile_d, "wb") as code:       
            code.write(r.content)             
         CSVtoJSON.JSONParser(Date)
 DBConnect.UpdateBatchDate(Date)


             

                        

      






