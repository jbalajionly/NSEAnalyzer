from pymongo import MongoClient
import os, shutil

def get_zipfile_location (command):

 try: 
    #const uri = "mongodb+srv://Test:Test@mrittiyogan-ycyvf.mongodb.net/test?retryWrites=true&w=majority";
    #client = MongoClient('localhost', 27017)
    #client = MongoClient("mongodb+srv://Test:Test@mrittiyogan-ycyvf.mongodb.net/test?retryWrites=true&w=majority")
    client = MongoClient("mongodb+srv://Test:Test@mrittiyogan-ycyvf.mongodb.net/test?retryWrites=true&w=majority")
    mydb = client["NSE"]
    mycol = mydb["config"]
    print("Connected successfully!!!") 



    myquery = { "file": command }
    mydoc = mycol.find(myquery)

    for x in mydoc:
     print(x)

    for key, value in x.items():
        if key=="location":
           print (key)
           print(value)     
           return value


 except:
    print("Could not connect to MongoDB") 


def folder_clear(folder):

 for the_file in os.listdir(folder):
    file_path = os.path.join(folder, the_file)
    try:
        if os.path.isfile(file_path):
            os.unlink(file_path)
        #elif os.path.isdir(file_path): shutil.rmtree(file_path)
    except Exception as e:
        print(e)


def get_table (command):

 try: 
    #client = MongoClient('localhost', 27017)
    client = MongoClient("mongodb+srv://Test:Test@mrittiyogan-ycyvf.mongodb.net/NSE?retryWrites=true&w=majority")
    mydb = client["NSE"]
    mycol = mydb["fconfig"]
    print("Connected successfully!!!") 


    myquery = { "file": command , "load":"Y"}
    mydoc = mycol.find(myquery)
    
    for x in mydoc:
      print(x)
    
    for key, value in x.items():
        if key=="table":
          print (key)
          print(value)
          return value
 except:
    print("Could not connect to MongoDB") 


def delete_table (table,date):

 try: 
    #client = MongoClient('localhost', 27017)
    client = MongoClient("mongodb+srv://Test:Test@mrittiyogan-ycyvf.mongodb.net/NSE?retryWrites=true&w=majority")
    mydb = client["NSE"]
    mycol = mydb[table]
    print("Connected successfully!!!") 

    myquery = { "DATE": date}
    mydoc = mycol.delete_many(myquery)
    print(mydoc.deleted_count)

 except:
    print("Could not connect to MongoDB") 

def getBatchDate():
 try: 
  #  client = MongoClient('localhost', 27017)
    client = MongoClient("mongodb+srv://Test:Test@mrittiyogan-ycyvf.mongodb.net/NSE?retryWrites=true&w=majority")
    mydb = client["NSE"]
    mycol = mydb["batchdate"]
    print("Connected successfully!!!") 

    myquery = {"completed":"N" }
    mydoc = mycol.find(myquery).sort("date")
    
    for x in mydoc:
        for key, value in x.items():
         if key=="date":
          print(value)
          return value     

    return 0

 except:
    print("Could not connect to MongoDB") 


def getRequestURL():
 try: 
    #client = MongoClient('localhost', 27017)
    client = MongoClient("mongodb+srv://Test:Test@mrittiyogan-ycyvf.mongodb.net/NSE?retryWrites=true&w=majority")
    mydb = client["NSE"]
    mycol = mydb["nseconfig"]
    print("Connected successfully!!!") 

    myquery = {"load":"Y" }
    mydoc = mycol.find(myquery)
    
    listURL=[]

    for x in mydoc:
        for key, value in x.items():
          if(key=="url"):        
           #print(value)
           listURL.append(value)          

    return listURL

 except:
    print("Could not connect to MongoDB") 


def get_file_date_format (command):

 try: 
    #client = MongoClient('localhost', 27017)
    client = MongoClient("mongodb+srv://Test:Test@mrittiyogan-ycyvf.mongodb.net/NSE?retryWrites=true&w=majority")
    mydb = client["NSE"]
    mycol = mydb["fconfig"]
    print("Connected successfully!!!") 



    myquery = { "file": command , "load":"Y" }
    mydoc = mycol.find(myquery)

    for x in mydoc:
      print(x)

    for key, value in x.items():
        if key=="dateformat":
          print (key)
          print(value)          
          return value
 except:
    print("Could not connect to MongoDB") 

def UpdateBatchDate(Batchdate):
 try: 
    #client = MongoClient('localhost', 27017)
    client = MongoClient("mongodb+srv://Test:Test@mrittiyogan-ycyvf.mongodb.net/NSE?retryWrites=true&w=majority")
    mydb = client["NSE"]
    mycol = mydb["batchdate"]
    print("Connected successfully!!!") 

    myquery = { "date":Batchdate }
    update = {"$set": {"completed":"Y"}}
    mycol.update_one(myquery,update)
    #mydoc = mycol.find(myquery).sort("date")
    for x in mycol.find():
      print(x)
  #  for x in mycol:
   #     for key, value in x.items():
    #     if key=="date":
     #     print(value)
      #    return value     

    return 0

 except:
   print("Could not connect to MongoDB") 
