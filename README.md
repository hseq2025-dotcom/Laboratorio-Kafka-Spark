from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, when

# 1. Crear la sesión de Spark para Batch Processing
spark = SparkSession.builder \
    .appName("BatchProcessing_HSEQ_Sensors") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Cargar el conjunto de datos (ejemplo desde un archivo CSV o fuente simulada)
# Reemplaza la ruta con tu archivo de datos reales
df_sensores = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("sensores_industriales.csv")

print("--- Esquema Original del Dataset ---")
df_sensores.printSchema()

# 3. Limpieza y Transformación de Datos
# Filtrar valores nulos y estandarizar rangos operativos
df_limpio = df_sensores.dropna().filter((col("temperatura") >= 0) & (col("temperatura") <= 100))

# 4. Análisis Exploratorio de Datos (EDA) y Agregaciones
print("--- Estadísticas Descriptivas por Sensor ---")
analisis_estadistico = df_limpio.groupBy("id_sensor") \
    .agg(
        avg("temperatura").alias("temp_promedio"),
        max("temperatura").alias("temp_maxima"),
        avg("humedad").alias("humedad_promedio")
    )

analisis_estadistico.show()

# 5. Clasificación de alertas operativas mediante lógica condicional
df_con_alertas = df_limpio.withColumn(
    "nivel_alerta",
    when(col("temperatura") > 75, "CRÍTICO")
    .when((col("temperatura") > 50) & (col("temperatura") <= 75), "ADVERTENCIA")
    .otherwise("ESTABLE")
)

print("--- Visualización de Resultados del Procesamiento en Batch ---")
df_con_alertas.select("id_sensor", "temperatura", "humedad", "nivel_alerta", "timestamp").show(10, truncate=False)
