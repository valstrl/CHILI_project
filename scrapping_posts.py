from bs4 import BeautifulSoup
import requests
import csv
import urllib.request
import os # added this import to process files/dirs
import re
import numpy as np
import pandas as pd

forum='http://forums.thefashionspot.com'
path=forum

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
        try:
            page= requests.get( url)
        except ConnectionError:
            print('--ConnectionError--')
            return df, keywords
        
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
                    try:
                        post_quote = [re.findall('(?:.html#post).*', x.find("a")['href'])[0][10::] for x in post_quote.find_all("div",{"class":"postby"}) if x.find("a") != None ]
                    except IndexError:
                        print('IndexError for post_quote')
                        print(post_quote)
                        break
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

df_thread=pd.read_csv('threads.csv')
    
for i in range(0, df_thread.shape[0]):
      
    thread=df_thread.copy().reset_index().iloc[i]
    
    new_thread_posts=pd.DataFrame()
    new_thread_posts, keywords= processPosts(thread['link'],new_thread_posts)
    
    #if all the post are not scrapped
    while int(new_thread_posts.tail(1)['number']) != int(thread['size'] +1):
        print('--In while loop--')
        #try again
        new_thread_posts=pd.DataFrame()
        new_thread_posts, keywords= processPosts(thread['link'],new_thread_posts)
           
    new_thread_posts['Thread ID'] =thread['ID']
    df_posts =df_posts.append(new_thread_posts)
    #print(df_thread.head())
    #df_thread=df_thread.set_index('ID')
    #print(df_thread.head())
    #print(new_thread_posts.head())
    
    #recover creation time , keyword in thread datasets
    
    df_thread.loc[i,'time']= new_thread_posts[new_thread_posts['number']==1]['time'].values[0]
    df_thread.loc[i,'keywords']= keywords
    
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



