#!/usr/bin/env python
# coding: utf-8

# In[19]:


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, DateType,ArrayType
from pyspark.sql.functions import col, split, explode, row_number, trim, udf, regexp_replace, upper,when
from pyspark.sql.window import Window
import ast, re

#Creamos las session de apache spark en una variable

spark = SparkSession.builder.getOrCreate()


# In[3]:


#Creamos las rutas de lectura y escritura
bucket_name = 'gs://dmc-proyecto-big-data-24' 
#Ingesta landing
ruta_landing_calendario = f"{bucket_name}/datalake/landing/calendarioagricola/"
ruta_landing_lugar = f"{bucket_name}/datalake/landing/lugar_/"
ruta_landing_producto = f"{bucket_name}/datalake/landing/productoagricola/"
ruta_landing_cliente = f"{bucket_name}/datalake/landing/Cliente/"

#Carga curated
ruta_curated_cliente = f"{bucket_name}/datalake/curated/Cliente/"
ruta_curated_c_agricola = f"{bucket_name}/datalake/curated/calendarioagricola"


# ### POBLANDO LA CAPA CURATED

# In[10]:


dfcliente_ok = spark.read.format("parquet").option("header","True").load(ruta_landing_cliente)


# In[13]:


#Limpiando el campo teléfono de guiones (-)
dfcliente_ok = dfcliente_ok.withColumn('TELEFONO', regexp_replace('TELEFONO','-',''))

#Eliminando los espacios en blanco
dfcliente_ok1 = dfcliente_ok.withColumn("ID", trim("ID")) #Eliminamos los espacios en blanco
dfcliente_ok2 = dfcliente_ok1.withColumn("NOMBRE", trim("NOMBRE"))#Eliminamos los espacios en blanco
dfcliente_ok3 = dfcliente_ok2.withColumn("TELEFONO", trim("TELEFONO")) #Eliminamos los espacios en blanco
dfhmcliente_curated = dfcliente_ok3.withColumn("CODLUGAR", trim("CODLUGAR"))#Eliminamos los espacios en blanco

dfhmcliente_curated.show(10)


# In[14]:


#Guardado en capa Curated
dfhmcliente_curated.write.mode("overwrite").format("parquet").save(ruta_curated_cliente)


# ## Procesando Calendario Agrícola para cargarlo en Curated

# In[16]:


#Creación de dfcalendario_agricola
df_c_agricola= spark.read.format("parquet").option("header","true").load(ruta_landing_calendario)


# In[17]:


df_c_agricola.show()


# In[20]:


df_c_agricola_curated = df_c_agricola.withColumn("Flag",when(col("estado").like("%C%"),"1").otherwise("0"))
df_c_agricola_curated.show()


# In[21]:


df_c_agricola_curated = df_c_agricola.withColumn("Flag",when(col("estado").like("%C%"),"1").otherwise("0"))
df_c_agricola_curated.show()


# In[22]:


df_c_agricola_curated = df_c_agricola_curated.select("codlugar","region","oficina","codproducto","cultivo","variedad","codmes","estado","Flag")
df_c_agricola_curated = df_c_agricola_curated.withColumn("Flag", col("Flag").cast(IntegerType()) )
df_c_agricola_curated.show()


# In[23]:


#Guardado en capa Curated
df_c_agricola_curated.write.mode("overwrite").format("parquet").save(ruta_curated_c_agricola)


# In[24]:


spark.stop()


# In[ ]:




