# -*- coding: utf-8 -*-
"""
Tarea 3 - Big Data (UNAD) - Procesamiento en batch con Apache Spark
Dataset: Siniestralidad vial de Santiago de Cali 2016-2024 (Datos Abiertos Cali)
Problema: identificar patrones de accidentalidad en Cali (tendencia anual,
gravedad, tipo de accidente, vehículos involucrados y calidad de los reportes).
Autor: Juan Diego Castro Mena
"""
import os
import sys

import matplotlib
matplotlib.use("Agg")  # sin interfaz gráfica: las figuras se guardan como PNG
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RUTA = sys.argv[1] if len(sys.argv) > 1 else "hdfs://localhost:9000/Tarea3/siniestralidad_cali.csv"
CARPETA_GRAFICOS = os.path.expanduser("~/tarea3/graficos")
os.makedirs(CARPETA_GRAFICOS, exist_ok=True)

spark = (SparkSession.builder
         .appName("Tarea3_Siniestralidad_Cali")
         # Con un solo nodo, 8 particiones de shuffle son suficientes (el valor por defecto es 200)
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def titulo(texto):
    print("\n" + "=" * 70)
    print(texto)
    print("=" * 70)


# ---------------------------------------------------------------------------
# 1. CARGA DEL CONJUNTO DE DATOS DESDE HDFS
# ---------------------------------------------------------------------------
titulo("1. CARGA DE DATOS")
df_raw = (spark.read
          .option("header", True)
          .option("sep", ";")
          .option("encoding", "ISO-8859-1")   # el archivo viene en Latin-1
          .csv(RUTA))

# El archivo trae columnas repetidas, así que se renombran todas
columnas = ["codigo", "fecha", "tipo_confirmado", "direccion", "clase_accidente",
            "clase_accidente_2", "tipo_confirmado_2", "medio_reporte",
            "num_vehiculos", "tipos_vehiculos"]
df_raw = df_raw.toDF(*columnas)

total_raw = df_raw.count()
print(f"Registros cargados: {total_raw:,}")
df_raw.printSchema()
df_raw.show(5, truncate=False)

# ---------------------------------------------------------------------------
# 2. LIMPIEZA
# ---------------------------------------------------------------------------
titulo("2. LIMPIEZA DE DATOS")

# 2.1 Quitar espacios y saltos de línea, y convertir '.', ',' y '' en nulos
df = df_raw
for c in columnas:
    limpio = F.trim(F.regexp_replace(F.col(c), r"[\r\n]", ""))
    df = df.withColumn(c, F.when(limpio.isin(".", ",", ""), None).otherwise(limpio))

# 2.2 Conteo de nulos por columna (antes de filtrar)
print("Valores nulos por columna:")
df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columnas]).show(truncate=False)

# 2.3 Eliminar filas sin fecha o sin tipo confirmado (filas vacías) y duplicados por código
df = df.filter(F.col("fecha").isNotNull() & F.col("tipo_confirmado").isNotNull())
df = df.dropDuplicates(["codigo"])

# 2.4 Unificar mayúsculas/minúsculas y corregir caracteres dañados
df = (df
      .withColumn("tipo_confirmado", F.upper("tipo_confirmado"))
      .withColumn("clase_accidente", F.upper("clase_accidente"))
      # "CA?DA OCUPANTE" y "CAIDA DE OCUPANTE" son la misma clase
      .withColumn("clase_accidente", F.when(F.col("clase_accidente").rlike(r"^CA.DA (DE )?OCUPANTE$"), "CAIDA DE OCUPANTE")
                                      .when(F.col("clase_accidente").isin("NO APLICA", "NOAPLICA"), None)
                                      .otherwise(F.col("clase_accidente")))
      # "TELEF?NICO 127", "TELEFONICO127" y "CAD123" se unifican
      .withColumn("medio_reporte", F.regexp_replace(F.upper("medio_reporte"), r"TELEF\?NICO", "TELEFONICO"))
      .withColumn("medio_reporte", F.regexp_replace("medio_reporte", r"([A-Z])(\d)", "$1 $2"))
      # algunos tipos de vehículo vienen con comillas sobrantes ("AUTOMOVIL, ""BUS PADRON""")
      .withColumn("tipos_vehiculos", F.upper(F.regexp_replace("tipos_vehiculos", '"', "")))
      .drop("clase_accidente_2", "tipo_confirmado_2"))   # columnas duplicadas

total_limpio = df.count()
print(f"Registros después de la limpieza: {total_limpio:,} "
      f"(se eliminaron {total_raw - total_limpio:,})")

# ---------------------------------------------------------------------------
# 3. TRANSFORMACIÓN
# ---------------------------------------------------------------------------
titulo("3. TRANSFORMACIÓN")
df = (df
      .withColumn("fecha", F.to_date("fecha", "d/M/yyyy"))
      .withColumn("num_vehiculos", F.col("num_vehiculos").cast("int"))
      .filter(F.col("fecha").isNotNull())
      .withColumn("anio", F.year("fecha"))
      .withColumn("mes", F.month("fecha"))
      .withColumn("dia_semana", F.dayofweek("fecha"))     # 1 = domingo ... 7 = sábado
      .withColumn("es_accidente", F.col("tipo_confirmado") != "NEGATIVO")
      .withColumn("gravedad",
                  F.when(F.col("tipo_confirmado").startswith("CON FALLECIDO"), "Con fallecido")
                   .when(F.col("tipo_confirmado") == "CON LESIONADO", "Con lesionado")
                   .when(F.col("tipo_confirmado") == "SOLO DAÑOS", "Solo daños")
                   .otherwise("Negativo (no fue accidente)"))
      .withColumn("involucra_moto", F.col("tipos_vehiculos").contains("MOTOCICLETA")))

df.cache()
df.printSchema()
df.show(5, truncate=False)

accidentes = df.filter("es_accidente")
total_acc = accidentes.count()

# ---------------------------------------------------------------------------
# 4. ANÁLISIS EXPLORATORIO (EDA)
# ---------------------------------------------------------------------------
titulo("4.1 Reportes por resultado de verificación")
por_gravedad = (df.groupBy("gravedad").count()
                .withColumn("porcentaje", F.round(F.col("count") * 100 / total_limpio, 2))
                .orderBy(F.desc("count")))
por_gravedad.show(truncate=False)

titulo("4.2 Reportes y accidentes confirmados por año")
por_anio = (df.groupBy("anio")
            .agg(F.count("*").alias("reportes"),
                 F.sum(F.col("es_accidente").cast("int")).alias("accidentes"),
                 F.sum((F.col("gravedad") == "Con lesionado").cast("int")).alias("con_lesionado"),
                 F.sum((F.col("gravedad") == "Con fallecido").cast("int")).alias("con_fallecido"))
            .orderBy("anio"))
por_anio.show(truncate=False)

titulo("4.3 Accidentes confirmados por mes")
por_mes = accidentes.groupBy("mes").count().orderBy("mes")
por_mes.show(12)

titulo("4.4 Accidentes confirmados por día de la semana")
nombres_dia = {1: "Dom", 2: "Lun", 3: "Mar", 4: "Mié", 5: "Jue", 6: "Vie", 7: "Sáb"}
por_dia = accidentes.groupBy("dia_semana").count().orderBy("dia_semana")
por_dia.show()

titulo("4.5 Clase de accidente (confirmados)")
por_clase = (accidentes.filter(F.col("clase_accidente").isNotNull())
             .groupBy("clase_accidente").count().orderBy(F.desc("count")))
por_clase.show(10, truncate=False)

titulo("4.6 Medio por el que se reportó")
con_medio = df.filter(F.col("medio_reporte").isNotNull())
print(f"Registros con medio de reporte informado: {con_medio.count():,} de {total_limpio:,}")
con_medio.groupBy("medio_reporte").count().orderBy(F.desc("count")).show(10, truncate=False)

titulo("4.7 Estadísticas del número de vehículos por accidente")
accidentes.select("num_vehiculos").summary("count", "mean", "stddev", "min", "50%", "max").show()

titulo("4.8 Tipos de vehículo involucrados (DataFrame: split + explode)")
vehiculos_df = (accidentes.filter(F.col("tipos_vehiculos").isNotNull())
                .withColumn("vehiculo", F.explode(F.split("tipos_vehiculos", ",")))
                .withColumn("vehiculo", F.trim("vehiculo"))
                .filter(F.col("vehiculo") != "")
                .groupBy("vehiculo").count().orderBy(F.desc("count")))
vehiculos_df.show(10, truncate=False)

titulo("4.9 Mismo conteo con RDD (flatMap + map + reduceByKey)")
try:
    rdd = accidentes.select("tipos_vehiculos").rdd
    top_rdd = (rdd.filter(lambda fila: fila[0] is not None)
               .flatMap(lambda fila: fila[0].split(","))
               .map(lambda v: (v.strip(), 1))
               .reduceByKey(lambda a, b: a + b)
               .takeOrdered(5, key=lambda x: -x[1]))
    for vehiculo, n in top_rdd:
        print(f"{vehiculo:<20} {n:,}")
except Exception as e:
    print("No se pudo ejecutar la parte RDD:", str(e)[:200])

titulo("4.10 Participación de motocicletas según gravedad")
moto = (accidentes.filter(F.col("tipos_vehiculos").isNotNull())
        .groupBy("gravedad")
        .agg(F.count("*").alias("accidentes"),
             F.sum(F.col("involucra_moto").cast("int")).alias("con_moto"))
        .withColumn("pct_con_moto", F.round(F.col("con_moto") * 100 / F.col("accidentes"), 2))
        .orderBy(F.desc("pct_con_moto")))
moto.show(truncate=False)

titulo("4.11 Direcciones con más accidentes confirmados")
(accidentes.filter(F.col("direccion").isNotNull())
 .groupBy(F.upper("direccion").alias("direccion")).count()
 .orderBy(F.desc("count")).show(10, truncate=False))

# ---------------------------------------------------------------------------
# 5. VISUALIZACIÓN
# ---------------------------------------------------------------------------
titulo("5. GENERACIÓN DE GRÁFICOS")
AZUL, GRIS, ROJO = "#2a6fb0", "#b8b8b8", "#c0392b"


def guardar(nombre):
    ruta = os.path.join(CARPETA_GRAFICOS, nombre)
    plt.tight_layout()
    plt.savefig(ruta, dpi=150)
    plt.close()
    print("Gráfico guardado:", ruta)


# 5.1 Reportes vs accidentes confirmados por año
filas = por_anio.collect()
anios = [str(r["anio"]) for r in filas]
reportes = [r["reportes"] for r in filas]
acc = [r["accidentes"] for r in filas]
plt.figure(figsize=(9, 5))
plt.bar(anios, reportes, color=GRIS, label="Reportes recibidos")
plt.bar(anios, acc, color=AZUL, label="Accidentes confirmados")
plt.title("Reportes y accidentes confirmados por año - Cali 2016-2024")
plt.xlabel("Año"); plt.ylabel("Cantidad"); plt.legend()
guardar("01_reportes_vs_accidentes_por_anio.png")

# 5.2 Distribución por resultado de verificación
filas = por_gravedad.collect()
plt.figure(figsize=(8, 4.5))
plt.barh([r["gravedad"] for r in filas][::-1], [r["count"] for r in filas][::-1], color=AZUL)
plt.title("Resultado de la verificación de los reportes")
plt.xlabel("Cantidad de reportes")
guardar("02_distribucion_gravedad.png")

# 5.3 Accidentes con fallecido por año
plt.figure(figsize=(9, 5))
plt.plot(anios, [r["con_fallecido"] for r in por_anio.collect()], marker="o", color=ROJO)
plt.title("Accidentes con fallecido por año")
plt.xlabel("Año"); plt.ylabel("Accidentes")
guardar("03_fallecidos_por_anio.png")

# 5.4 Clase de accidente
filas = por_clase.limit(8).collect()
plt.figure(figsize=(8, 4.5))
plt.barh([r["clase_accidente"] for r in filas][::-1], [r["count"] for r in filas][::-1], color=AZUL)
plt.title("Clase de accidente (accidentes confirmados)")
plt.xlabel("Cantidad")
guardar("04_clase_accidente.png")

# 5.5 Tipos de vehículo
filas = vehiculos_df.limit(8).collect()
plt.figure(figsize=(8, 4.5))
plt.barh([r["vehiculo"] for r in filas][::-1], [r["count"] for r in filas][::-1], color=AZUL)
plt.title("Vehículos involucrados en accidentes confirmados")
plt.xlabel("Cantidad")
guardar("05_tipos_vehiculo.png")

# 5.6 Accidentes por día de la semana
filas = por_dia.collect()
plt.figure(figsize=(8, 4.5))
plt.bar([nombres_dia[r["dia_semana"]] for r in filas], [r["count"] for r in filas], color=AZUL)
plt.title("Accidentes confirmados por día de la semana")
plt.ylabel("Cantidad")
guardar("06_dia_semana.png")

# 5.7 Accidentes por mes
filas = por_mes.collect()
plt.figure(figsize=(9, 5))
plt.plot([r["mes"] for r in filas], [r["count"] for r in filas], marker="o", color=AZUL)
plt.xticks(range(1, 13))
plt.title("Accidentes confirmados por mes (2016-2024)")
plt.xlabel("Mes"); plt.ylabel("Cantidad")
guardar("07_accidentes_por_mes.png")

titulo("RESUMEN")
print(f"Reportes analizados:     {total_limpio:,}")
print(f"Accidentes confirmados:  {total_acc:,} ({total_acc * 100 / total_limpio:.1f} %)")
print(f"Gráficos en:             {CARPETA_GRAFICOS}")

spark.stop()
