import csv
import sys
import os # added this import to process files/dirs
import re
import numpy as np
import pandas as pd
import string

import nltk
#nltk.download('averaged_perceptron_tagger')
from nltk.tag import pos_tag
#stemming
from nltk import word_tokenize          
from nltk.stem import PorterStemmer 

from sklearn.feature_extraction import text
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer

from tqdm import tqdm

class PorterTokenizer(object):
    def __init__(self):
        self.porter= PorterStemmer()
    def __call__(self, doc):
        return [self.porter.stem(t) for t in word_tokenize(doc)]
    
#strip any proper nouns (NNP) or plural proper nouns (NNPS) from a text
def keep_nouns(text):
    tagged = pos_tag(text.split()) #use NLTK's part of speech tagger
    non_propernouns = [word for word,pos in tagged if pos != 'NNP' and pos != 'NNPS']
    return ' '.join(non_propernouns)

#keep propers nouns
def keep_propers(text):
    tagged = pos_tag(text.split()) #use NLTK's part of speech tagger
    propernouns = [word for word,pos in tagged if pos == 'NNP' or pos == 'NNPS']
    return ' '.join(propernouns)

print("Loading dataset...")

path='../data/posts_2.csv'
data= pd.read_csv(path,parse_dates=['time'])

threads=pd.DataFrame(columns=['time', 'Thread ID', 'text'])
for name, group in tqdm(data.groupby(by='Thread ID')):
    text=group['post'].tolist()
    text=' '.join(text)
    time=group[group['number']==1]['time'].values[0]
    threads= threads.append(pd.DataFrame([[time,name,text]], columns=['time', 'Thread ID', 'text']))

print('Dataset size: {}'.format(data.size))

from sklearn.feature_extraction import text

months=['january', 'february', 'march','april', 'may','june', 'july','august', 'september','october','november','december']
stop_words_plus=['com', 'http','www','t','s','fr','vs', 'https', 'html','w','l', 'thank', 'source', 'post','thread','know','lot', 'forum', 'got', 'facebook', 'credit', 'id',  'bit', 'ly', 'anybody', 'jpg', 'u', 'net', 'kz', 'uk']
authors=data['author']
pronouns=['versace','vogue','christian','nowfashion', 'balenciaga', 'mak', 'stylebistro','voguerunway', 'dior', 'instagram','dolce', 'dolce gabbana', 'gabbana','ccp','yves','saint', 'fenrost', 'mistress_f', 'carol', 'carol poell','poell','simon', 'nowfashion.com', 'thefashionspot', 'van', 'givenchy', 'buro247']
my_stop_words = text.ENGLISH_STOP_WORDS.union(months, stop_words_plus,pronouns,string.punctuation,authors)
my_stop_words=my_stop_words.union([PorterStemmer().stem(t) for t in my_stop_words])



n_features = 30000
n_components =  10  
n_top_words = 5
#remove proper names
dataset=threads['text']
dataset=dataset.map(keep_nouns)

data_samples= dataset

# For LDA we need: 
#tf_vectorizer = CountVectorizer
#tf = tf_vectorizer.fit_transform(data_samples)
#tf_feature_names = tf_vectorizer.get_feature_names()

min_df= 1 #used for removing terms that appear too infrequently, removing words that appear in less than X% of the docs / X docs
max_df=0.99 #used for removing terms that appear too frequently, removing words that appear in more than X% of the docs/ X docs
# Use tf (raw term count) features for LDA.
print("Extracting tf features for LDA...")
tf_vectorizer = CountVectorizer(max_df=max_df, min_df=min_df,
                                max_features=n_features,stop_words=set(my_stop_words)
                                , analyzer='word', ngram_range=(1,3),tokenizer=PorterTokenizer())
tf = tf_vectorizer.fit_transform(data_samples)

tf_feature_names = tf_vectorizer.get_feature_names()
tf_vocab_= tf_vectorizer.vocabulary_

vocab=pd.Series(tf_vocab_)
vocab.to_csv('data/vocabulary.csv')

#save matrix
import scipy.sparse
scipy.sparse.save_npz('data/tf_matrix.npz',tf)

# Proper Nouns
my_stop_words = text.ENGLISH_STOP_WORDS.union(months, stop_words_plus,string.punctuation,authors)
my_stop_words=my_stop_words.union([PorterStemmer().stem(t) for t in my_stop_words])



#keep proper names
print("Keeping only the proper nouns")
data_samples = dataset.map(keep_propers)


min_df= 1
max_df=0.99

print("Extracting tf features for proper nouns..")
tf_vectorizer = CountVectorizer(max_df=max_df, min_df=min_df,
                                max_features=n_features,stop_words=set(my_stop_words)
                                ,token_pattern=u'(?ui)\\b\\w*[a-z]+\\w*\\b', ngram_range=(1, 3))


tf_proper = tf_vectorizer.fit_transform(data_samples)

tf_feature_names_propers = tf_vectorizer.get_feature_names()
tf_vocab_proper= tf_vectorizer.vocabulary_


vocab_proper=pd.Series(tf_vocab_proper)
vocab_proper.to_csv('data/vocabulary_proper.csv')

#save matrix
scipy.sparse.save_npz('data/tf_porper_matrix.npz',tf_proper)