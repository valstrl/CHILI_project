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
    
    folderURLs=[ url + x.find_all("a")[0]["href"]  for x in folderData]

    #print(len(folderData)) = 27 ! OK!  
    for url in folderURLs:
        found_next=True
        page_nb = 0
        print(url)
        
        while found_next == True: #and page_nb != 3:
            page= requests.get( url)
            soup= BeautifulSoup(page.text,'html.parser')

            threadData= soup.find_all("tbody",id=re.compile("^threadbits_forum_"))[0].find_all("tr")

            titles = []
            links=[]
            authors = []
            #creation_times=[]
            #keywords=[]

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

            
            nextPage= soup.find_all("a",{"rel": "next"})
            if len(nextPage)==0:
                found_next= False 
            else:
                page_nb += 1
                url= forum + nextPage[0]['href']
    return df

df_thread= pd.DataFrame(columns=['ID','time', 'author', 'title', 'link', 'keywords','size'])

forum='http://forums.thefashionspot.com'
path=forum
print("Processing threads from " + path +")...")
df_thread=processThread(path,df_thread) 
print("End processing  threads from " + path +")...")
#df_thread.tail()


# scrapping 1 page of posts in the forum

""" PostDataset
Post ID ok
textual content ok
Author’s pseudo ok 
Posting time and date/order ok 
Tag : if someone is tag in the message write the Pseudo , else None <-
Thread ID ok
"""

def processPosts(url, df):
    found_next=True
    authors=[]
    while found_next== True:
    
        ''' take the data from an html file and append to our csv file '''
        page= requests.get( url)
        soup= BeautifulSoup(page.text,'html.parser')

        postData = soup.find_all(id="posts")

        metaData = soup.find_all("meta")
        #print(metaData)
        keywords= soup.find_all(attrs={"name": "keywords"})[0]['content']
        #print(keywords)
        
        for post in postData[0].find_all("table",id=re.compile("^post")):
            
            post_message = post.find(id=re.compile("^post_message_"))
            post_message=BeautifulSoup(str(post_message)).get_text().translate(str.maketrans("\n\t\r", "   ")).strip('[]')            
            
            post_id= post.find(id=re.compile("^post_message_"))['id'][13::]
            
            post_quote= post.find(id=re.compile("^post_message_")).find("td",{"class":"quote"})
            
            post_quotes=[]
            if  post_quote != None:
                if post_quote.find("div",{"class":"postby"}) != None:
                    #print(post_quote.find_all("div",{"class":"postby"}))
                    post_quote = [re.findall('(?:.html#post).*', x.find("a")['href'])[0][10::] for x in post_quote.find_all("div",{"class":"postby"}) if x.find("a") != None ]
                    #print(post_quote)
                    #post_quote=re.findall('(?:.html#post).*',post_quote)[0][10::]
                else:
                    post_quote= []
            else:
                post_quote= []
            
            post_author = post.find("a", {"class": "bigusername"})
            post_author = BeautifulSoup(str(post_author)).get_text().translate(str.maketrans("\n\t\r", "   ")).strip('[]')
            
            tag=[]
            
            if authors == []:
                authors.append(post_author)
            else:
                if post_author not in authors:
                    authors.append(post_author)
                for author in authors:
                    if author  in post_message.split():
                        tag.append(author)

 

            post_date=post.find("td", {"class": "thead"})
            post_date= BeautifulSoup(str(post_date)).get_text().translate(str.maketrans("\n\t\r", "   ")).strip('[]')
            
            post_number=post.find("div", {"class": "thread-number"})
            nb=re.findall('\d+',(BeautifulSoup(str(post_number)).get_text().translate(str.maketrans("\n\t\r", "   ")).strip('[]')))
            post_number= int(nb[0]) # get date
            
            df = df.append(pd.DataFrame([[post_id,post_message,post_date, post_number, post_author, post_quote, tag]],columns=['ID','post','time','number', 'author', 'quote', 'tag']), ignore_index=True)

            nextPage= soup.find_all("a",{"rel": "next"})
            if len(nextPage)==0:
                found_next= False 
            else:
                url= forum + nextPage[0]['href']
    return df, keywords

                
#forum='http://forums.thefashionspot.com'
#thread='http://forums.thefashionspot.com/f60/alexander-wang-f-w-2018-19-new-york-297129.html'
#path=thread
nextPage=True

df_posts=pd.DataFrame(columns=['ID','post','time','number', 'author','Thread ID', 'quote', 'tag'])
print("Processing post ...")

    
for i in range(0, df_thread.shape[0]):
      
    thread=df_thread.copy().reset_index().iloc[i]
    #print(thread['link'])
    
    new_thread_posts=pd.DataFrame()
    new_thread_posts, keywords= processPosts(thread['link'],new_thread_posts)
    new_thread_posts['Thread ID'] =thread['ID']
    df_posts =df_posts.append(new_thread_posts)
    #print(df_thread.head())
    #df_thread=df_thread.set_index('ID')
    #print(df_thread.head())
    #print(new_thread_posts.head())
    
    #recover creation time , keyword in thread datasets
    
    df_thread.set_value(i,'time', new_thread_posts[new_thread_posts['number']==1]['time'].values[0])
    df_thread.set_value(i,'keywords', keywords)
    
    #print(df_thread.head())

df_posts=df_posts.set_index('ID')
print("End processing post ...")
csvFile = "posts.csv"
df_posts.to_csv(csvFile)
print("End writing post")

# our csv file
csvFile = "threads.csv"
df_thread.to_csv(csvFile)
print("End writing threads...")





