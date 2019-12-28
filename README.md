# There are three PySpark program files in this repository

## 1. Graphs.py
This program uses motif in GraphFrames to find unique triangles in a graph of vertices and edges
For example:
Input:
vertices:
1,martin
2,ricky
3,tom
4,jack
5,john

edges
1,2,friend
2,3,follow
3,1,friend
1,5,follow


Output: unique triangles:

 1 --> 2 --> 3 --> 1
 
## 2. ML-email.py
This is a program that uses linear regression to predict spam and non-spam emails

## 3. anagram_bigram.py
This program finds top anagrams, bigrams and unique words in a piece of text file.
1. For top unique words and top anagrams, any word (after dropping special characters) with less than 4 characters is ignored.
2. for bigrams just drop the first and last special character, if any, are dropped.
