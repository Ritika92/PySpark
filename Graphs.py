
# coding: utf-8

# In[ ]:


#!C:\Users\ritika\Anaconda3\python
from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession


# In[3]:


from functools import reduce
from pyspark.sql.functions import col, lit, when
from graphframes import *


# In[ ]:


if __name__ == '__main__':

    if len(sys.argv) != 3:  
        print("Usage: prog4.py <input-file>", file=sys.stderr)
        exit(-1)

    # create an instance of SparkSession
    spark = SparkSession        .builder        .appName("")        .getOrCreate()
    #
    print("spark=",  spark)

#
    # read name of input file
    input_v = sys.argv[1]
    print("input path : ", input_v)
    
    input_e = sys.argv[2]
    print("input path : ", input_e)
    
    df_v = spark          .read          .format("csv")          .option("header","false")          .option("inferSchema", "true")          .load(input_v)
    
    df_e = spark          .read          .format("csv")          .option("header","false")          .option("inferSchema", "true")          .load(input_e)
    
    #create vertices and edges of the graph
    vertices = df_v.selectExpr("_c0 as id", "_c1 as name")
    edges = df_e.selectExpr("_c0 as src", "_c1 as dst","_c2 as relationship")
    vertices.show()
    edges.show()
    
    #create the graph frame
    g = GraphFrame(vertices, edges)
    #find triangular relations  
    results = g.find("(A)-[]->(B); (B)-[]->(C); (C)-[]->(A)")
    results.show()
    
    #filter out duplicate relations that could have a different start points to only retain unique triangles of a given graph
    R= results.filter('A.id<B.id and B.id<C.id')
    R.show()
    
    
    

