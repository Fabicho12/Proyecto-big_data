#!/usr/bin/env python
# coding: utf-8

# In[24]:


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, DateType, ArrayType,IntegerType
from pyspark.sql.functions import col, split, explode, row_number, trim, monotonically_increasing_id, col, expr,udf, concat_ws, when
from pyspark.sql.window import Window
import ast, re
from functools import reduce

#Creamos las session de apache spark en una variable
spark = SparkSession.builder.getOrCreate()


# In[26]:


#Creamos las rutas de lectura y escritura
bucket_name = 'gs://dmc-proyecto-big-data-24' 

#Ingesta workload
ruta_work_load= f"{bucket_name}/datalake/workload/FastAPI/csv/CalendarioAgricola/hm_agricola.csv"
ruta_work_load_cliente= f"{bucket_name}/datalake/workload/FastAPI/csv/OportunidadCliente/h_cliente.csv"

#Carga landing
ruta_landing_calendario = f"{bucket_name}/datalake/landing/calendarioagricola/"
ruta_landing_lugar = f"{bucket_name}/datalake/landing/lugar_/"
ruta_landing_producto = f"{bucket_name}/datalake/landing/productoagricola/"
ruta_landing_cliente = f"{bucket_name}/datalake/landing/Cliente/"


# ## POBLANDO CAPA LANDING

# ### Construcción de la tabla HM_AGRICOLA

# In[27]:


#Definimos schema para el dfhm_agricola
df_schema = StructType([
StructField("codmes",StringType(),False),
StructField("codlugar",StringType(),False),
StructField("region",StringType(),False),
StructField("oficina",StringType(),False),
StructField("codproducto",StringType(),False),
StructField("cultivo",StringType(),False),
StructField("variedad",StringType(),False),
StructField("estado",StringType(),True),

])

#Creación de dfhmagricola
dfhmagricola= spark.read.csv(ruta_work_load,header = True,schema = df_schema,encoding="UTF-8",sep = ",")


# In[28]:


#Conteo inicial antes de alguna transformación
dfhmagricola.count()
dfhmagricola.show()


# In[29]:


#Conversión del dato tipo cadena a una lista de la column OFICINA
dfhmagricola= dfhmagricola.withColumn("oficina",split(col("oficina"),',')) 
#Conversión de lista a filas
dfhmagricola = dfhmagricola.withColumn("oficina",explode(col("oficina")))
dfhmagricola.show()


# In[30]:


# Definir una función UDF para aplicar re.split() a la columna 'Columna'
def split_column(value):
    return re.split(r'\by\b', value.strip())
split_udf = udf(split_column, ArrayType(StringType()))
#Conversión del dato tipo cadena a una lista de la columna oficina
dfhmagricola = dfhmagricola.withColumn("oficina",split_udf('oficina'))
dfhmagricola = dfhmagricola.withColumn("oficina",explode(col("oficina")))
dfhmagricola.show()


# In[31]:


#Conversión del dato tipo cadena a una lista de la column VARIEDAD
dfhmagricola = dfhmagricola.withColumn("variedad",split(col("variedad"),','))
dfhmagricola.show()
#Conversión de lista a filas
dfhmagricola = dfhmagricola.withColumn("variedad",explode(col("variedad")))
dfhmagricola.show()


# In[32]:


dfhmagricola.count() #9156 registros con las divisiones


# In[33]:


# Definir una función UDF para aplicar re.split() a la columna 'Columna'
def split_column(value):
    return re.split(r'\by\b', value.strip())
split_udf = udf(split_column, ArrayType(StringType()))
#Conversión del dato tipo cadena a una lista de la column VARIEDAD
dfhmagricola = dfhmagricola.withColumn("variedad",split_udf("variedad"))
dfhmagricola = dfhmagricola.withColumn("variedad",explode(col("variedad")))
dfhmagricola.show()


# In[34]:


#Eliminamos los espacios en blanco
dfhmagricola = dfhmagricola.withColumn("variedad", trim("variedad"))
dfhmagricola = dfhmagricola.withColumn("oficina",trim("oficina"))
dfhmagricola = dfhmagricola.withColumn("region",trim("region"))
dfhmagricola = dfhmagricola.withColumn("cultivo",trim("cultivo"))
dfhmagricola = dfhmagricola.withColumn("codlugar",trim("codlugar"))
dfhmagricola = dfhmagricola.withColumn("codproducto",trim("codproducto"))
#Eliminamos celdas vacias
dfhmagricola = dfhmagricola.filter((col("oficina") != '')) 
dfhmagricola = dfhmagricola.filter((col("variedad") != '')) 
#Guardado de data procesada
dfhmagricola = dfhmagricola
#Impresión de las filas del dfhmagricola procesada
print(dfhmagricola.count())



# In[36]:


dfhmagricola = dfhmagricola.na.drop(how="any")


# In[41]:


dfhmagricola.show(truncate = False)


# In[42]:


#Creción de vista para consulta
dfhmagricola.createOrReplaceTempView("v_calendario_agricola")
#Guardado en capa landing
dfhmagricola.repartition(2).write.mode("overwrite").format("parquet").save(ruta_landing_calendario)


# ### Construcción de la tabla MM_lugar

# In[44]:


#Selección de columas para la creación de la dfMM_lugar proveniente de la dfhmagricola
dfMM_lugar = dfhmagricola.select(col("codlugar"),col("region"),col("oficina")).distinct().orderBy("codlugar","region","oficina")
dfMM_lugar.show()


# In[45]:


#Conteo de filas del dfMM_lugar procesado
print(dfMM_lugar.count())
#Creación de vista para consulta
dfMM_lugar.createOrReplaceTempView("v_lugar")
#Guardado en capa landing
dfMM_lugar.repartition(2).write.mode("overwrite").format("parquet").save(ruta_landing_lugar)


# ### Construcción de la tabla MM_producto

# In[47]:


#Selección de columas para la creación de la dfMM_producto proveniente de la dfhmagricola
dfMM_producto = dfhmagricola.select(col("codproducto"),col("cultivo"),col("variedad")).distinct().orderBy("codproducto","cultivo","variedad")
dfMM_lugar.show()


# In[48]:


#Conteo de filas del dfMM_producto_agricola procesado
print(dfMM_producto.count())
#Creación de vista para consulta
dfMM_producto.createOrReplaceTempView("v_productoagricola")
#Guardado en capa landing
dfMM_producto.repartition(2).write.mode("overwrite").format("parquet").save(ruta_landing_producto)


# ### Construcción de la tabla HM_CLIENTE

# In[49]:


df_schema_cliente = StructType([
StructField("ID",StringType(),False),
StructField("NOMBRE",StringType(),False),
StructField("TELEFONO",StringType(),False),
StructField("CODLUGAR",StringType(),False),
StructField("CODPRODUCTO",StringType(),False),
StructField("MES",StringType(),False),
])

#Creación de dfcalendario_agricola
dfhmcliente= spark.read.csv(ruta_work_load_cliente,header = True,schema = df_schema_cliente,encoding="UTF-8",sep = ";")


# In[50]:


dfhmcliente.show()


# In[51]:


dfhmcliente_landing = dfhmcliente.select("ID","NOMBRE","TELEFONO","CODLUGAR")
dfhmcliente_landing.show(5)


# In[52]:


#Guardado en capa landing
dfhmcliente_landing.write.mode("overwrite").format("parquet").save(ruta_landing_cliente)


# In[53]:


spark.stop()

