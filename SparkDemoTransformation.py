
# coding: utf-8

# In[1]:


sc


# In[2]:


from pyspark import SparkContext
from pyspark import HiveContext


# In[4]:


simple_data =sc.parallelize([1,'Alice',23])


# In[6]:


data=([1,'Alice',23])


# In[7]:


simple_data =sc.parallelize(data)


# In[30]:


#simple_data.collect()
#simple_data.take(3)
#simple_data.count()
#simple_data.first()
#simple_data.last()


# In[12]:


data= [1,2,3,4,5]
simple_data = sc.parallelize(data)
modified_data=simple_data.map(lambda x: x*2)


# In[31]:


modified_data.collect()


# In[15]:


filtered_data=simple_data.filter(lambda x: x%2==0)


# In[32]:


filtered_data.collect()


# In[33]:


#simple_data.intersection(modified_data).collect()


# In[34]:


#simple_data.union(modified_data).collect()


# In[35]:


#simple_data.subtract(modified_data).collect()

