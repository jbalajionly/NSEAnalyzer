import calendar

def parseURL(url,Date):  
   #print(y) 
   #print(Date)
   YYYY=Date[:4]
   #print(YYYY)
   YY=Date[2:4]
   #print(YY)
   MM=Date[4:6] 
   #print(MM)
   DD=Date[6:8]
   #print(DD)
   MON=str(calendar.month_abbr[int(MM)]).upper()
   #print(MON)
   strURL = str(url)
   
   while(strURL.find('<') != -1):
      startindex=strURL.find('<')
      endindex=strURL.find('>')
      #print(strURL.find('<'))
      Dateformat=strURL[startindex:endindex+1]
      if(Dateformat=="<YYYY>"):
        strURL=strURL.replace(Dateformat,YYYY)        
        #print(strURL)        
      elif(Dateformat=="<DDMMYY>"):
        strURL=strURL.replace(Dateformat,str(DD+MM+YY))
        #print(strURL)  
      elif(Dateformat=="<MON>"):
        strURL=strURL.replace(Dateformat,str(MON))
        #print(strURL)
      elif(Dateformat=="<DDMONYYYY>"):
        strURL=strURL.replace(Dateformat,str(DD+MON+YYYY)) 
        #print(strURL) 
        
   if(strURL.find('<') == -1):
     #ParsedList.append(strURL) 
     #print(strURL)
      return strURL  
        
 