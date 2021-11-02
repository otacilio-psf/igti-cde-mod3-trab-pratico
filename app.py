#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# In[2]:


print("======================= SparkSession Starting ========================")
spark = SparkSession.builder.appName("trab-pratico-de-igti").getOrCreate()


# In[ ]:


sc = spark.sparkContext
sc.setLogLevel("ERROR")


# In[3]:


print("============================== Read data =============================")
df_titles = spark.read.csv('data/title_basics.tsv', header=True, sep='\t')
df_ratings = spark.read.csv('data/title_ratings.tsv', header=True, sep='\t')


# In[4]:


print("========================== Investigate data ==========================")

df_titles.printSchema()
df_titles.show(5)


# In[5]:


df_ratings.printSchema()
df_ratings.show(5)


# In[6]:


df_titles.select('titleType').distinct().show()


# In[7]:


print("Quantos filmes (incluindo os da televisão) foram lançados no ano de 2015?")
print(
    df_titles
    .filter(col('titleType').isin('tvMovie', 'movie'))
    .filter(col('startYear')=='2015')
    .count()
)


# In[8]:


print("Qual o gênero de títulos mais frequente?")
(
    df_titles
    .withColumn('genres', split(col('genres'),','))
    .select('*', explode(col('genres')).alias('unique_g'))
    .groupBy('unique_g').count()
    .orderBy(col('count').desc())
    .show()
)


# In[9]:


print("Qual o gênero com a melhor nota média de títulos?")

(
    df_titles
    .join(df_ratings, 'tconst')
    .withColumn('genres', split(col('genres'),','))
    .select('*', explode(col('genres')).alias('unique_g'))
    .groupBy('unique_g').agg(mean(col('averageRating')).alias('media'))
    .orderBy(col('media').desc())
    .show()
)


# In[10]:


print("Qual o vídeo game do gênero aventura mais bem avaliado em 2020?")
(
    df_titles
    .filter(col('titleType')=='videoGame')
    .filter(col('startYear')=='2020')
    .join(df_ratings, 'tconst')
    .withColumn('genres', split(col('genres'),','))
    .select('*', explode(col('genres')).alias('unique_g'))
    .filter(col('unique_g')=='Adventure')
    .orderBy(col('averageRating').desc())
    .show()
)


# In[11]:


print("Qual o percentual de títulos do gênero comédia lançados em 2018 em relação ao total de títulos lançados nesse ano?")
print("ERRADO!!!!!")
from pyspark.sql.window import Window

w = Window.partitionBy()

(
    df_titles
    .filter(col('startYear')=='2018')
    .withColumn('genres', split(col('genres'),','))
    .select('*', explode(col('genres')).alias('unique_g'))
    .groupBy('unique_g').count()
    .withColumn('percent', (col('count')/sum(col('count')).over(w))*100)
    .filter(col('unique_g')=='Comedy')
    .show()
)


# In[12]:


print("Qual o percentual de títulos do gênero comédia lançados em 2018 em relação ao total de títulos lançados nesse ano?")
print("CORRETO!!!!")
(
    df_titles
    .filter(col('startYear')=='2018')
    .withColumn('genres', split(col('genres'),','))
    .withColumn('comedy', when(array_contains(col('genres'), 'Comedy'),1).otherwise(0))
    .select((sum(col('comedy'))/count(col('comedy')))*100)
    .show()
)


# In[13]:


print("""
Considere a definição de uma udf abaixo: 

def sqr_divide(value): 

    return (value**2)/2 

sqr_divide_udf = udf(sqr_divide, IntegerType())

A definição de sqr_divide_udf possui um problema. Depois de solucionar o problema, ao executar 
""")

from pyspark.sql.types import DoubleType

def sqr_d(v):
    return (v**2)/2

sqr_d_udf = udf(sqr_d, DoubleType())

(
    df_ratings
    .withColumn('averageRating', col('averageRating').cast(DoubleType()))
    .select(sqr_d_udf('averageRating').alias('averageRating'))
    .agg(mean('averageRating').alias('averageRating'))
    .show()
)


# In[14]:


print("Deseja-se utilizar um join para retornar somente as linhas referentes a títulos que estão sem nota, isto é, não aparecem no df_ratings")
df_titles.join(df_ratings, 'tconst', 'anti').show()


# In[15]:


spark.stop()
print("============================== Finished ==============================")

