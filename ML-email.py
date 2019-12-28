
# coding: utf-8

# In[17]:


# coding: utf-8

# In[ ]:


#!C:\Users\ritika\Anaconda3\python

from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithSGD


# In[ ]:

#Function for formatting output
def ret(x):
    if (x==0):
        return 'Non-Spam'
    elif(x==1):
        return 'Spam'


if __name__ == '__main__':

    if len(sys.argv) != 4:  
        print("Usage: bonus.py <input-file>", file=sys.stderr)
        exit(-1)

    # create an instance of SparkSession
    spark = SparkSession        .builder        .appName("bonus-assignment")        .getOrCreate()
    #
    print("spark=",  spark)

#
   # read name of input file
    input_email = sys.argv[1]
    input_s = sys.argv[2]
    input_ns = sys.argv[3]
	
	
	#upload data in files
	spam = spark.sparkContext.textFile(input_s)
	nspam = spark.sparkContext.textFile(input_ns)
	test = spark.sparkContext.textFile(input_email)
	
  

# In[19]:

    #map each word into an element 
    s_words = spam.map(lambda x : x.split(" "))
    ns_words = nspam.map(lambda x: x.split(" "))
	test_data = test.map(lambda x:x.split("\t"))
    t=test_data.mapValues(lambda x:x.split())


# In[20]:


    #map text to vectors of features
    tf = HashingTF(numFeatures=256)
    s_features = tf.transform(s_words)
    ns_features = tf.transform(ns_words)
	test_features = t.mapValues(lambda x :tf.transform(x))


# In[21]:


    #LabeledPoint datasets for spam and non-spam
    spam_a = s_features.map(lambda x:LabeledPoint(1, x))
    nspam_a = ns_features.map(lambda x:LabeledPoint(0, x))


# In[22]:


    #combining spam and non-spam data
    samples = spam_a.union(nspam_a)
   
   
# In[23]:


    #Logistic Regression model
    model = LogisticRegressionWithSGD().train(samples)


# In[1]:


    #prediting on new data
    predictions = test_features.mapValues(lambda x:model.predict(x))


# In[26]:

    #format output
    output = predictions.mapValues(ret)
	output.collect()

