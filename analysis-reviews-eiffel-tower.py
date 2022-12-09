# in case can't find pyspark in sys.path or in site-packages
import findspark
findspark.init()

from operator import itemgetter
from pyspark.sql.functions import count, col, regexp_replace, lower, trim
from pyspark.sql import SparkSession


def most_texted_words(spark):
    spark.sql('select explode(split(text," ")) as palavras from reviews').withColumn('palavras',lower(regexp_replace(col('palavras'),'[^a-zA-Z ]',''))).filter('length(palavras)>0').groupBy('palavras').agg(count('palavras').alias('quantidade')).orderBy(col('quantidade').desc()).show()

def most_used_expressions(spark):
    spark.sql("select trim(text) as expressao,count(*) as referencias from ((select explode(SPLIT(trim(text),'\\\\.')) as text from reviews)) as x where length(trim(text))>1 and size(filter(SPLIT(text,' '),s -> length(s)>0))>1 and trim(text) is not null group by trim(text) order by count(*) desc").show(10,truncate=False)

def reviews_best_topics(spark):
    pass

def time_distribution(spark):
    fmt= "date_format(to_date(trim(createdAt),'MMM dd,yyyy'),'YYYY-MM')"
    sql = spark.sql(f"select {fmt} as created_at,count({fmt}) as references from reviews group by {fmt} order by extract(year from to_date(trim(created_at),'yyyy-MM')) asc,extract(month from to_date(trim(created_at),'yyyy-MM')) asc")
    sql.show(500,truncate=False)
    
    # pdf = sql.toPandas()
    # pdf.plot.line(x='createdAt',y='references')

def topics(spark):
    df = spark.sql("select title from reviews").withColumn('title',trim(lower(regexp_replace(col('title'),'[^a-zA-Z ]','')))).groupBy('title').agg(count('title').alias('count')).orderBy(col('count').desc()).select('title').withColumnRenamed('title','topics').show(20,truncate=False)



if __name__ == '__main__':
    spark = SparkSession.builder.master('local[1]').appName('analysis-tweets-eiffel-tower').getOrCreate()
    spark.sql('CREATE OR REPLACE TEMPORARY VIEW reviews USING json OPTIONS (path "eiffel-tower-reviews.json")')
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    print("\n----------------------------------------\n")
    print("Encontre as palavras mais utilizadas nas avaliações.")
    most_texted_words(spark)
    print("\n----------------------------------------\n")
    print("Encontre as expressões mais usadas. Considere uma expressão um conjunto de palavras na sequencia. O tamanho da sequencia pode ser determinado por você")
    most_used_expressions(spark)
    print("\n----------------------------------------\n")
    print("Encontre os principais tópicos relacionados às revisões.")
    topics(spark)
    print("\n----------------------------------------\n")
    print("Mapeie a distribuição temporal das revisões")
    time_distribution(spark)    