#!/usr/bin/env python
# coding: utf-8

# In[9]:


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, DateType, ArrayType,IntegerType
from pyspark.sql.functions import col, split, explode, row_number, trim, monotonically_increasing_id, col, expr,udf, concat_ws, when, regexp_replace, upper
from pyspark.sql.window import Window
#from unidecode import unidecode
import ast, re
from functools import reduce

#Creamos las session de apache spark en una variable

spark = SparkSession.builder.getOrCreate()


# In[2]:


#Creamos las rutas de lectura y escritura
bucket_name = 'gs://dmc-proyecto-big-data-24' 
#Ingesta landing
ruta_landing_lugar = f"{bucket_name}/datalake/landing/lugar_/"
#Ingesta curated
ruta_curated_c_agricola = f"{bucket_name}/datalake/curated/calendarioagricola" 
ruta_curated_distrito = f"{bucket_name}/datalake/curated/mm_distritos_peru" 
ruta_curated_cliente = f"{bucket_name}/datalake/curated/Cliente/"
#Carga functional
ruta_functional_c_agricola = f"{bucket_name}/datalake/functional/calendarioagricola"
ruta_functional_localidad = f"{bucket_name}/datalake/functional/localidad"


# In[3]:


df_c_agricola_functional = spark.read.format("parquet").option("header","True").load(ruta_curated_c_agricola)
df_c_agricola_functional = df_c_agricola_functional.select(col("CODLUGAR").alias("COD_LUGAR"), col("region").alias("REGION"), col("oficina").alias("OFICINA"), col("codproducto").alias("COD_PRODUCTO"), col("cultivo").alias("CULTIVO"), col("variedad").alias("VARIEDAD"), col("codmes").alias("COD_MES"), col("estado").alias("ESTADO"), col("Flag").alias("FLAG"))
df_c_agricola_functional.show()


# In[4]:


dfcliente_curated = spark.read.format("parquet").option("header","True").load(ruta_curated_cliente) 
df_c_agricola_functional = dfcliente_curated.join(df_c_agricola_functional, dfcliente_curated.CODLUGAR == df_c_agricola_functional.COD_LUGAR)
df_c_agricola_functional.show()


# In[5]:


df_c_agricola_functional_ok = df_c_agricola_functional.select(col("ID"),col("NOMBRE"), col("TELEFONO"), col("COD_LUGAR"), col("REGION"), col("OFICINA"), col("COD_PRODUCTO"), col("CULTIVO") ,col("VARIEDAD"), col("COD_MES"), col("ESTADO"), col("FLAG"))
print("El número de registros del DataFrame df_c_agricola_functional_ok es: ", df_c_agricola_functional_ok.count())
df_c_agricola_functional_ok.show()


# In[7]:


df_c_agricola_functional_ok.write.mode("overwrite").partitionBy("COD_MES").format("parquet").save(ruta_functional_c_agricola)


# ## Join de m_lugar con distrito

# In[ ]:


#Leyendo distrito
df_distrito= spark.read.format("parquet").option("header","true").load(ruta_curated_distrito)
df_distrito.show()


# In[ ]:


#Leyendo m_lugar
df_lugar= spark.read.format("parquet").option("header","true").load(ruta_landing_lugar)
df_lugar.show()


# In[ ]:


df_lugar = df_lugar.withColumn('oficina',upper(col('oficina')))
df_lugar.show()


# In[ ]:


df_localidad_functional = df_distrito.join(df_lugar, df_distrito.distrito == df_lugar.oficina)
df_localidad_functional.show()


# In[ ]:


df_localidad_functional_ok = df_localidad_functional.select(col("codlugar").alias("COD_LUGAR"),col("region").alias("REGION"),col("codprovinvia").alias("COD_PROVINCIA"),col("provinvia").alias("PROVINCIA"),col("coddistrito").alias("COD_DISTRITO"),col("distrito").alias("DISTRITO"),col("capital").alias("CAPITAL"),col("ubigeo").alias("UBIGEO"),col("poblacion").alias("POBLACION"),col("fechadata").alias("FECHA_DATA"),col("origdata").alias("ORIG_DATA"),col("fecactualizacion").alias("FEC_ACTUALIZACION"),col("coddepartamento").alias("COD_DEPARTAMENTO"),col("departamento").alias("DEPARTAMENTO"),col("latitud").alias("LATITUD"),col("longitud").alias("LONGITUD"))
df_localidad_functional_ok.show()


# In[ ]:


df_localidad_functional_ok = df_localidad_functional_ok.withColumn('REGION',upper(col('REGION')))
df_localidad_functional_ok.show()


# In[ ]:


df_localidad_functional_ok.count()


# In[ ]:


df_localidad_functional_ok.show(48)
df_localidad_functional_ok.write.mode("overwrite").format("parquet").save(ruta_functional_localidad)


# In[ ]:


spark.stop()


# In[ ]:




