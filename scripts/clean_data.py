import pandas as pd
import datetime as dt
import time
from nltk.stem import PorterStemmer,WordNetLemmatizer
from nltk.tokenize import RegexpTokenizer


LOCAL_DIR='/tmp/'
def main():

    subred=pd.read_csv(LOCAL_DIR + 'subreddit_data.csv')

    subred['timestamp']=pd.to_datetime(subred['timestamp'])

    tokenizer=RegexpTokenizer(r'\w+')
    p_stemmer=PorterStemmer

    stemmed_title=[]

    for title in subred['title']:
        tokens=tokenizer.tokenize(title.lower())
        stemmer=[p_stemmer.stem(token) for token in tokens]
        stemmed_title.append(" ".join(stemmer))

    subred['stemmed_title']=stemmed_title

    subred.to_csv(LOCAL_DIR + 'subreddit_clean.csv')



