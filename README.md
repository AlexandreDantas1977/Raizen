# Raizen
Solução de leitura de dados da ANP, engenharia de dados

Uso do software LibreOffice, único que consegui visualizar as abas de dados da planilha original: vendas-combustiveis-m3.xls

Tentativa de ler em python, com algumas bibliotecas, o arquivo em formato .XLS falhou

Foi necessário um trabalho manual de separar as abas em arquivos em formato .CSV
	
As abas DPCache_m3, DPCache_m3_2 e DPCache_m3_3 apresentaram dados e colunas equivalentes, assim juntei todas numa única aba para leitura das informações.
	
As abas DPCache_m3_4, DPCache_m3_5 e DPCache_m3_6 apresentaram quebra nas formações nas linhas com ano 2020, acabei desprezando essas abas pois a única informação diferente era a coluna de “SEGMENTO“ em relação as outras que tinha  a coluna “COMBUSTIVEL (product)”.

Código de leitura e tratamento de dados
Nota: os valores totalizados por ano e mês não ficaram batidos com a planilha, portanto o indicador de Variação acumulado também não bateu


# Bibliotecas:

from pyspark.sql.functions import split, col, row_number, expr, row_number, current_timestamp
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark import HiveContext
from pyspark.sql import Row, functions as F, types as T
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, split, input_file_name, substring, regexp_replace, trim, when, add_months, to_date, concat, coalesce, months_between, max, sum, count, avg
from datetime import datetime, timedelta
from functools import reduce


#Código Python

#definição de parametros
SystemPath = 'TI/XLS_CVS_teste'
FileName = 'XLS_CVS_teste'
DatePath = '2023/10/16/07/00'

df = spark.read.load('abfss://landing-zone@romagnoledatalake1.dfs.core.windows.net/z_Lab/anp_prod_ok.csv', format='csv', header=True, sep=";")
colunas_a_manter = ['product', 'year', 'unit', 'uf']
#realizando pivot das colunas de Meses para Linhar
df_melted = df.select(colunas_a_manter + [expr("stack(12, 'JAN', JAN, 'FEV', FEV, 'MAR', MAR, 'ABR', ABR, 'MAI', MAI, 'JUN', JUN, 'JUL', JUL, 'AGO', AGO, 'SET', SET, 'OUT', OUT, 'NOV', NOV, 'DEZ', DEZ) as (month, volume)")])
#tipagem de campo e geração de campo Mês numérico (para ordenação)
df = df_melted\
    .withColumn('year', col('year').cast('int'))\
    .withColumn('volume',when(col('volume').isNotNull(), col('volume')).otherwise(0).cast('float'))\
    .withColumn('created_at',current_timestamp())\
    .withColumn('month_no',\
        when(col('month') == 'JAN', 1).\
        when(col('month') == 'FEV', 2).\
        when(col('month') == 'MAR', 3).\
        when(col('month') == 'ABR', 4).\
        when(col('month') == 'MAI', 5).\
        when(col('month') == 'JUN', 6).\
        when(col('month') == 'JUL', 7).\
        when(col('month') == 'AGO', 8).\
        when(col('month') == 'SET', 9).\
        when(col('month') == 'OUT', 10).\
        when(col('month') == 'NOV', 11).\
        when(col('month') == 'DEZ', 12).\
        otherwise(0).cast('int'))     
   
#filtro de registros apenas com valores válidos
df = df.filter(df['volume'].cast('string').rlike('^[0-9.]+$'))
#display(df.limit(12)

#gravar dados na Consume Zone
df.write.mode('overwrite').save(CZ_path, format='parquet')

#Agrupamento e pivotar dados
df_pivot = df.groupBy("month", "month_no").pivot("year").agg(sum("volume")).orderBy("month_no")

agg_cols = df_pivot.columns[1:]
rollup_df = df_pivot.rollup().sum()

renamed_df = reduce(
    lambda rollup_df, idx: rollup_df.withColumnRenamed(rollup_df.columns[idx], agg_cols[idx]), 
    range(len(rollup_df.columns)), rollup_df
)

renamed_df = renamed_df.withColumn('month', lit('Total'))

df_pivot.unionByName(
    renamed_df
)

colunas = df_pivot.columns[2:-1]

# Crie uma janela de especificação para a acumulação
window_spec = Window.orderBy()  # Você pode especificar a ordem conforme necessário

# Calcule a soma acumulativa das duas últimas colunas
for coluna in colunas[-2:]:
    df_pivot = df_pivot.withColumn(coluna + '_cumsum', sum(col(coluna)).over(window_spec))

# Calcule a nova coluna acumulativa com base nas duas últimas colunas
df_pivot = df_pivot.withColumn('variacao acumulado 19-20',
                   when((col(colunas[-2] + '_cumsum') + col(colunas[-1] + '_cumsum') == 0), "n/d")
                   .otherwise((col(colunas[-1] + '_cumsum') / col(colunas[-2] + '_cumsum') - 1) * 100))

# Mostre o DataFrame resultante
df_pivot.show()

#display(final_df.limit(12))


#visualizações, filtros e agrupamentos de dados

#agrupamento e sumarização de valor, por ESTADO, PRODUTO, ANO e MES
df_agregado = df.groupBy("uf", "product", "year", "month_no", "month").agg(sum("volume").alias("TOTAL")).orderBy("uf", "product", "year", "month_no")
display(df_agregado.limit(12))

#agrupamento e sumarização de valor, por ESTADO, ANO e MES
df_agregado = df.groupBy("uf", "year", "month_no", "month").agg(sum("volume").alias("volume")).orderBy("uf", "year", "month_no")
display(df_agregado.limit(12))

#agrupamento e sumarização de valor, por PRODUTO, ANO e MES
df_agregado = df.groupBy("product", "year", "month_no", "month").agg(sum("volume").alias("volume")).orderBy("year", "month_no", "product")
display(df_agregado.limit(12))


#filtro por Produto = DIESEL e agrupamento e sumarização de valor, por PRODUTO, ANO e MES
df_diesel = df.filter(col("product").like("%DIESEL%"))
df_agregado = df_diesel.groupBy("product", "year", "month_no", "month").agg(sum("volume").alias("accum_total")).orderBy("year", "month_no", "product")
display(df_agregado.limit(12))


# Use a função sum e over para calcular o total acumulado por mês e ano.
#window_spec = Window.partitionBy("MES").orderBy("ANO").rowsBetween(Window.unboundedPreceding, 0)
#df_totalizado = df.withColumn("accum_total", sum("volume").over(window_spec))
#display(df_totalizado.limit(12))
