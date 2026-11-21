# Tarea 3 – Procesamiento de datos con Apache Spark

**Curso:** Big Data (202016911) – UNAD
**Estudiante:** Juan Diego Castro Mena

## Problema

Identificar patrones de accidentalidad vial en Santiago de Cali entre 2016 y 2024: qué tanto de lo reportado termina siendo un accidente real, cómo evolucionan los casos por año, qué clases de accidente predominan y qué vehículos están más involucrados en los casos graves.

## Conjunto de datos

**Siniestralidad vial 2016–2024**, publicado por la Alcaldía de Santiago de Cali en el portal Datos Abiertos Cali.

- Archivo: `Siniestralidad_2016_2024.csv` (26 MB, 371.025 registros, separador `;`, codificación ISO-8859-1)
- Fuente: https://datos.cali.gov.co/dataset/cb62a408-9029-4331-8815-ca2caeb126c0/resource/e0572389-cc41-4c1f-b443-862be10b6cc3/download/siniestralidad_2016_2024.csv

## Solución

El script `tarea3_batch.py` usa PySpark para:

1. **Cargar** el CSV desde HDFS.
2. **Limpiar** los datos: valores `.` como nulos, saltos de línea de Windows, filas vacías, duplicados, mayúsculas/minúsculas, caracteres dañados y comillas sobrantes.
3. **Transformar**: convierte la fecha y crea año, mes, día de la semana, gravedad y una marca de si el accidente involucra motocicleta.
4. **Analizar (EDA)**: resultado de la verificación de los reportes, tendencia anual, mes, día de la semana, clase de accidente, medio de reporte, número y tipo de vehículos (con DataFrames y con RDD: `filter`, `flatMap`, `map`, `reduceByKey`, `takeOrdered`), participación de motocicletas según gravedad y direcciones con más casos.
5. **Visualizar**: genera 7 gráficos PNG en `~/tarea3/graficos`.

## Principales resultados

- El 66,3 % de los reportes (246.107) se verificó como "Negativo", es decir, no correspondió a un accidente.
- Se confirmaron 124.889 accidentes: 74.029 solo con daños, 47.500 con lesionados y 3.360 con fallecidos.
- Las motocicletas aparecen en el 76,5 % de los accidentes con fallecido y en el 72,5 % de los que dejaron lesionados, frente al 23,8 % de los de solo daños.
- Los accidentes con fallecido se mantienen entre 360 y 450 por año entre 2016 y 2023.

## Entorno

- Ubuntu Server ARM64 en UTM (Mac con Apple Silicon)
- Hadoop (HDFS) en modo pseudo-distribuido
- Apache Spark 3.5.8 y OpenJDK 11
- Python 3.11 para PySpark (entorno `~/venv311`), con matplotlib

## Ejecución

```bash
# 1. Descargar el dataset
wget -O siniestralidad_cali.csv "https://datos.cali.gov.co/dataset/cb62a408-9029-4331-8815-ca2caeb126c0/resource/e0572389-cc41-4c1f-b443-862be10b6cc3/download/siniestralidad_2016_2024.csv"

# 2. Cargarlo a HDFS (usuario hadoop, con HDFS iniciado)
hdfs dfs -mkdir -p /Tarea3
hdfs dfs -put -f siniestralidad_cali.csv /Tarea3/

# 3. Preparar Python 3.11 (PySpark 3.5 no es compatible con Python 3.14)
uv venv -p 3.11 ~/venv311
uv pip install -p ~/venv311 matplotlib
export PYSPARK_PYTHON=$HOME/venv311/bin/python
export PYSPARK_DRIVER_PYTHON=$HOME/venv311/bin/python

# 4. Ejecutar
spark-submit tarea3_batch.py
```

También se puede indicar otra ruta de entrada como argumento:

```bash
spark-submit tarea3_batch.py file:///ruta/local/siniestralidad_cali.csv
```

## Estructura

```
tarea3_batch.py      Script de procesamiento en batch
salida_batch.txt     Salida completa de la ejecución
graficos/            Gráficos generados
README.md
```
