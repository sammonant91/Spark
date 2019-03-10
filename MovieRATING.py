
# coding: utf-8

# In[1]:


from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext


# In[2]:


#Find 
MovieLens=sc.textFile("u.data")


# In[5]:


#MovieRating=MovieLens.map(lambda x: ((int(x.split("\t")[0])),int(x.split("\t")[2])))
MovieRating=MovieLens.map(lambda x: (int(x.split("\t")[2])))


# In[12]:


MovieRating2.collect()


# In[10]:


#MovieRating1=MovieRating.countByValue()


# In[11]:


#MovieRating2=MovieRating1.map(lambda x: (x[1], x[0]))
MovieRating2=MovieRating.map(lambda x: (x,1))


# In[11]:


MovieRating3=MovieRating2.reduceByKey(lambda x,y:(x+y)).sortByKey(False).collect()


# In[19]:


MovieLens.collect()

