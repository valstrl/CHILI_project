from bs4 import BeautifulSoup
import requests
import csv
import urllib.request
import os # added this import to process files/dirs
import re
import numpy as np
import pandas as pd

""" Thread dataset
Title ok
link ok
ID ok
Forum ID ok
Creation time ok
Author ok
keywords ok
"""

def processThread( url, df ):
    ''' take the data from an html file and append to our csv file '''
    page= requests.get( url)
    soup= BeautifulSoup(page.text,'html.parser')

    folderData = soup.find_all("td",{"class":"alt1Active"})
    
    folderNames = [x.find_all("strong")[0].get_text()  for x in folderData]
    folderURLs=[ url + x.find_all("a")[0]["href"]  for x in folderData]
    
    #print('Folders names')
    #print(folderNames)

    #print(len(folderData)) = 27 ! OK!  
    for i, url in enumerate(folderURLs):
        found_next=True
        page_nb = 0
        print(url)
        
        while found_next == True or thread_number <= 150: #and page_nb != 3:
            page= requests.get( url)
            soup= BeautifulSoup(page.text,'html.parser')
            folder=folderNames[i]

            threadData= soup.find_all("tbody",id=re.compile("^threadbits_forum_"))[0].find_all("tr")

            titles = []
            links=[]
            authors = []
            
            
            #creation_times=[]
            #keywords=[]
            thread_number=0
            for thread in threadData:
                try:
                    #print(thread.find_all("td", {"class": "alt1 number-post"})[0].get_text())
                    thread_size=int(thread.find_all("td", {"class": "alt1 number-post"})[0].get_text().replace(',', ''))
                except:
                    thread_size=0
                if thread_size > 100:
                    thread_title = thread.find(id=re.compile("^thread_title_"))
                    thread_title=BeautifulSoup(str(thread_title)).get_text().translate(str.maketrans("\n\t\r", "   ")).strip('[]')


                    thread_id = thread.find(id=re.compile("^thread_title_"))['id'][13::]


                    thread_author = thread.find("td", id=re.compile("^td_threadtitle_")).find_all("div", {"class": "smallfont"})
                    thread_author =BeautifulSoup(str(thread_author)).get_text().translate(str.maketrans("\n\t\r", "   ")).strip('[]')

                    thread_link= forum +thread.find(id=re.compile("^thread_title_"))["href"] 
                    df = df.append(pd.DataFrame([[thread_id,'',thread_author,thread_title,thread_link, thread_size]],columns=['ID','time', 'author', 'title', 'link', 'size']), ignore_index=True)
                    thread_number= thread_number+1
            df['folder']=folder
            df['folder_number']=i
            #print(thread_number)
            nextPage= soup.find_all("a",{"rel": "next"})
            if len(nextPage)==0:
                found_next= False 
            else:
                page_nb += 1
                url= forum + nextPage[0]['href']
    return df

df_thread= pd.DataFrame(columns=['ID','time','folder','folder_number', 'author', 'title', 'link', 'keywords','size'])

forum='http://forums.thefashionspot.com'
path=forum
print("Processing threads from " + path +")...")
df_thread=processThread(path,df_thread) 
print("End processing  threads from " + path +")...")
#df_thread.tail()

# our csv file
csvFile = "threads.csv"
df_thread.to_csv(csvFile)
print("End writing threads...")



