
# coding: utf-8

# In[1]:


#!C:\Users\ritika\Anaconda3\python
from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 


# In[2]:


import re
#Function to convert input word to lower case and remove special characters
def fun_low_strip(rec):
    a = rec.lower()
    char_lst = ('-', '_', ',', '.', '?', ';', '/','%', '!', '#', '^', '@', '&', '\'','(',')','[',']',':','{','}')
    if a.startswith(char_lst):
        a = a[1:] 
    if a.endswith(char_lst):
        a = a[:-1]
    return a


#Function to sort word according to alphabetsand return the word as key and 1 as value for anagrams
def sort1(rec):
    r=''.join(sorted(fun_low_strip(rec)))
    return (r,1)
    
#Function to pass each word in a sentence in the function fun_low_strip for bigrams    
def big(x):
    test = []          
    for i in x:
        a= fun_low_strip(i)
        test.append(a)
    return test


#Function to pass every word to the function fun_low_strip for any word
def word(x):
    return (fun_low_strip(x),1)   

#Function to swap the positions of the key and the value in a key-value pair
def ret(x):
    a=[]
    for i in x:
        x= (i[1],i[0])
        a.append(x)
    return a


# In[ ]:


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: prog3.py <input-file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession        .builder        .appName("PythonWordCount")        .getOrCreate()
    
    input_path = sys.argv[1]
    print("input path : ", input_path)
    
    myrdd = spark.sparkContext.textFile(input_path)
    
    #Flatten out the input words
    rdd1 = myrdd.flatMap(lambda x : x.split())

    #To find anagrams, the seperated out input words are sorted(after removal of special characters & conversion to lower case), returned as a key value pair of(word,1) from mapper & the ones smaller than a length of 4 are filtered out.
    #After which all the key value pairs are reduced on the key(word) & then again mapped with the key value pair being swapped as (<word count>,<word>)
    #This new key value pair is then sort on the key(word count) & then the top 5 frequencies are selected out.
    anagram = rdd1.map(lambda x : sort1(x)).filter(lambda x : len(x[0])>3).reduceByKey(lambda x,y: x+y).map(lambda x:(x[1],x[0])).sortByKey(False).take(5)


    #To find Bigrams,the input is split using a mapper with each element being one sentence. The output of which is converted to lowers case
    #and special characters are removed through map function. The out after this is passed into flatmap to return conscutive words in the sentence as key and 1 as value.
    #After which all the key value pairs are reduced on the key(word) & then again mapped with the key value pair being swapped as (<word count>,<word>)
    #This new key value pair is then sort on the key(word count) & then the top 5 frequencies are selected out.
    bigrams = myrdd.map(lambda s : s.split()).map(lambda s:big(s)).flatMap(lambda s: [((s[i],s[i+1]),1) for i in range (0, len(s)-1)]).reduceByKey(lambda x, y : x+y).map(lambda x:(x[1],x[0])).sortByKey(False).take(5)    


    #To find frequency of any word,the seperated out input words are(after removal of special characters & conversion to lower case), returned as a key value pair of(word,1) from mapper & the ones smaller than a length of 4 are filtered out.
    #After which all the key value pairs are reduced on the key(word) & then again mapped with the key value pair being swapped as (<word count>,<word>)
    #This new key value pair is then sort on the key(word count) & then the top 5 frequencies are selected out.
    any_word = rdd1.map(lambda x : word(x)).filter(lambda x: len(x[0])>3).reduceByKey(lambda x,y : x+y).map(lambda x:(x[1],x[0])).sortByKey(False).take(5)
    
 

    print ("top-5 words:")
    print (ret(any_word))
    print ("top-5 anagrams:")
    print (ret(anagram))
    print ("top-5 bigrams:")
    print (ret(bigrams))
    
    spark.stop()

