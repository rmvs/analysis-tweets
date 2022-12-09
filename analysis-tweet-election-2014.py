# in case can't find pyspark in sys.path or in site-packages
import findspark
findspark.init()

from pyspark.sql.window import Window
from pyspark.sql.functions import count, explode, split, col, expr,to_timestamp, col,date_format, row_number,trim,transform
from pyspark.sql import SparkSession


def hashtags_periods_of_day(df):
    created_at = date_format( to_timestamp(col("created_at"),"EEE MMM d HH:mm:ss z yyyy") ,'HH:mm:ss').alias('created_at')
    _df = df.filter('content like "%#_%"').withColumn('content',explode(expr('filter(split(content," "),s -> s like "%#%")'))).withColumn('created_at',created_at).select('created_at','content')
    _df.filter('created_at >= "06:00:00" and created_at < "12:00:00"').groupBy('content').agg(count('content').alias('ocorrencias')).orderBy(col('ocorrencias').desc()).show(1)
    _df.filter('created_at >= "12:00:00" and created_at < "18:00:00"').groupBy('content').agg(count('content').alias('ocorrencias')).orderBy(col('ocorrencias').desc()).show(1)
    _df.filter('created_at >= "18:00:00" and created_at < "23:59:00"').groupBy('content').agg(count('content').alias('ocorrencias')).orderBy(col('ocorrencias').desc()).show(1)

def tweets_per_hour_day(df):
    created_at = date_format( to_timestamp(col("created_at"),"EEE MMM d HH:mm:ss z yyyy") ,'dd/MM/yyyy')
    df.select('created_at').withColumn('created_at',created_at).groupBy(col('created_at')).agg(count('created_at').alias('quantidade')).withColumn('quantidade',col('quantidade')/24).withColumnRenamed('created_at','Dia').withColumnRenamed('quantidade','tweets por hora').filter('created_at is not null').show(truncate=False)

def most_used_hashtag_per_day(df):
    created_at = date_format( to_timestamp(col("created_at"),"EEE MMM d HH:mm:ss z yyyy") ,'d').alias('dia')
    WindowDia = Window.partitionBy('dia').orderBy(col('q').desc())
    df.filter('content like "%#_%"').select(created_at,'content').withColumn('content',explode(expr('filter(split(content," "),s -> s like "%#%")'))).groupBy('dia','content').agg(count('content').alias('q'))\
    .withColumn('row',row_number().over(WindowDia)).filter(col('row')==1).drop('row').withColumnRenamed('q','quantidade').filter('dia is not null').show(truncate=False)

def most_used_words(df,w):
    df.select('content').filter(f'lower(content) like lower("%{w}%")').withColumn('content',explode(transform(split('content','\\\\.'),lambda x: trim(x)))).filter('length(content)>1').groupBy('content').agg(count('content').alias('referencias')).orderBy(col('referencias').desc()).show(10,truncate=False)


if __name__ == '__main__':
    spark = SparkSession.builder.master('local[1]').appName('analysis-tweets-election-2014').getOrCreate()

    columns = []
    with open('./columns.txt','r') as cols:
        columns = [ c.rstrip() for c in cols ]

    df = spark.read.csv('debate-tweets.tsv', sep=r'\t')
    for i in range(len(columns)):
        df = df.withColumnRenamed(f"_c{i}",columns[i])

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    # twitter create_at date pattern
    # EEE MMM d HH:mm:ss z yyyy

    print("\n-------------------------------------------\n")
    print("Quais foram as hashtags mais usadas pela manhã, tarde e noite?")
    hashtags_periods_of_day(df)
    print("\n-------------------------------------------\n")
    print("Quais as hashtags mais usadas em cada dia?")
    most_used_hashtag_per_day(df)
    print("\n-------------------------------------------\n")
    print("Qual o número de tweets por hora a cada dia?")
    tweets_per_hour_day(df)
    print("\n-------------------------------------------\n")
    print("Quais as principais sentenças relacionadas à palavra “Dilma”?")
    most_used_words(df,'dilma')
    print("\n-------------------------------------------\n")
    print("Quais as principais sentenças relacionadas à palavra “Aécio”?")
    most_used_words(df,'aecio')

