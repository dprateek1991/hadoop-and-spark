#####################################################################################
# Author        : Prateek Dubey
# Purpose       : Script to generate HIVE DDL's
#####################################################################################

from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,HiveContext,Row
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
import sys
import datetime
import os
import argparse
import subprocess

#Database Name Check

parser = argparse.ArgumentParser(description='Check if Database Name is inputted')
parser.add_argument('DB_NAME', type=str, help='Enter a Database Name for which DDL needs to be generated')
args = parser.parse_args()

print("Database Name:{}".format(sys.argv[1]))

db_name = sys.argv[1]

if os.system('hadoop fs -test -e s3://prateek/hive_ddl') != 0:
    print("Directory does not exist")
else:
    print("Directory exists")
    subprocess.call(["hadoop","fs","-rm","-r","s3://prateek/hive_ddl"])

if os.system('hadoop fs -test -e /user/hive/warehouse/hive_ddl') != 0:
    print("Directory does not exist")
else:
    print("Directory exists")
    subprocess.call(["hadoop","fs","-rm","-r","/user/hive/warehouse/hive_ddl"])

def main(sc, SQLContext):

    sqlContext = HiveContext(sc)

    sc.setLogLevel("ERROR")

    query = "USE {0}"

    query = query.format(db_name)

    sqlContext.sql(query)

    tables = sqlContext.sql("SHOW TABLES")

    tableNames = tables.select("tableName").rdd.map(lambda r: r)

    tableNames = tableNames.map(lambda x: x.tableName).collect()

    tableNames = [str(i) for i in tableNames]

    schema_empty_df = StructType([StructField("table_ddl",StringType(),True)])

    empty_df = sqlContext.createDataFrame(sc.emptyRDD(), schema_empty_df)

    df1 = empty_df

    for i in tableNames:
        show_query = "show create table "+i
        drop_query = "drop table "+i+";\n"
        describe_query = "describe formatted "+i
        seperator = ";\n"
        try:
            rdd = sc.parallelize([drop_query])
            newRDD = rdd.map(lambda x:{"table_ddl":x})
            newDF = rdd.map(lambda p: Row(table_ddl=p)).toDF()
            df = df1.unionAll(newDF)
            desc = sqlContext.sql(describe_query)
            desc_1 = desc.select(['data_type']).where("col_name='Location'")
            desc_2 = desc_1.rdd.map(lambda x:x.data_type).collect()
            desc_3 = [str(i) for i in desc_2]
            desc_4 = ''.join(desc_3)
            df0 = sqlContext.sql(show_query)
            show_1 = df0.rdd.map(lambda x:x.createtab_stmt).collect()
            show_2 = [str(i) for i in show_1]
            show_3 = ''.join(show_2)
            if show_3.find("LOCATION '") < 0:
                loc_query = "LOCATION '"+desc_4+"'"+"\n TBLPROPERTIES ("
                final_create_table=show_3.replace("TBLPROPERTIES (", loc_query)
            else:
                final_create_table = show_3
            list_final = [final_create_table]
            rdd_create_table = sc.parallelize(list_final)
		      	df_create_table = rdd_create_table.map(lambda p: Row(create_table_ddl=p)).toDF()
            df1 = df.unionAll(df_create_table)
            rdd1 = sc.parallelize([seperator])
            newRDD1 = rdd1.map(lambda x:{"delim":x})
            newDF1 = sqlContext.createDataFrame(newRDD1, ["delim"])
            df1 = df1.unionAll(newDF1)
        except:
            pass

    s3_output_path="s3://prateek/hive_ddl/"+db_name
    df1.repartition(1).write.mode('append').options(delimiter='\n').text(s3_output_path)

if __name__ == "__main__":
    conf = SparkConf().setMaster("yarn-client")
    conf = conf.setAppName("ddl_generator")
    sc   = SparkContext(conf=conf)

main(sc, SQLContext)
