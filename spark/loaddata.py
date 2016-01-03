from pyspark import SparkContext

sc=SparkContext()
# sc is an existing SparkContext.
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import StringType

sqlContext = SQLContext(sc)
import urllib
from datetime import timedelta, date
def load_day(filename, mydate, mymonth):
    # Load a text file and convert each line to a Row.
    udf = UserDefinedFunction(lambda x: urllib.unquote(x), StringType())
    d1 = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter"," ").option("quote",chr(0)).load(filename)
    d2=d1.selectExpr(mymonth +" as month", mydate + " as day","upper(C0) as project","C1 as url","cast(C2 as int) as pageview").groupBy("month","day","project","url").agg({"pageview": "sum"}).withColumnRenamed("sum(pageview)", "pageviews")
    d3 = d2.withColumn('url', udf(d2.url))
    #wiki.count()
    # Infer the schema, and register the DataFrame as a table.
    # Save to MySQL
    # Write to parquet file - if needed
    d3.write.partitionBy("month","day").parquet("/data/flash/spark/wikifull/t17","append")
mount = "/data/opt/wikistat/"
d= date(2015, 1, 1)
end_date = date(2016, 1, 1)
delta = timedelta(days=1)
sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
while d < end_date:
    print d.strftime("%Y-%m-%d")
    filename=mount + "pagecounts-2015" + d.strftime("%m") + d.strftime("%d") + "-*.gz"
    print(filename)
    #load_day(filename, d.strftime("%Y-%m-%d"))
    load_day(filename, d.strftime("%d"), d.strftime("%m"))
    d += delta
