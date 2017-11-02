- Importing all tables from RDBMS to Hive

sqoop import-all-tables \
-m 1 \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username=retail_dba \
--password=cloudera \
--compression-codec=snappy \
--as-parquetfile \
--warehouse-dir=/user/hive/warehouse \
--hive-import



- Importing one table from RDBMS to Hive

sqoop import \
-m 1 \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username=retail_dba \
--password=cloudera \
--table customers \
--compression-codec=snappy \
--as-parquetfile \
--warehouse-dir=/user/hive/warehouse \
--hive-import



- Importing one table from RDBMS to HDFS

sqoop import \
-m 1 \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username=retail_dba \
--password=cloudera \
--table customers \
--compression-codec=snappy \
--as-parquetfile \
--target-dir=/user/cloudera/prateek \



- Importing one table from RDBMS to HDFS with m set to default (default mapper is 4)

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username=retail_dba \
--password=cloudera \
--table customers \
--compression-codec=snappy \
--as-parquetfile \
--target-dir /user/cloudera/prateek \



- Exporting one table from HDFS to RDBMS 

Step 1: Create a contact file 

contact.csv
1,MahendraSingh,Dhoni,mahendra@bcci.com
2,Virat,Kohali,virat@bcci.com
5,Sachin,Tendulkar,sachin@bcci.com

Step 2: Push the file in HDFS

hdfs dfs -put contact.csv /user/cloudera/

Step 3: Create a table in Hive

create table contact
(contactId Int, 
firstName String, 
lastName String,
email String)
row format delimited
fields terminated by ','
stored as textfile;

Step 4: Load the file in managed table

load data inpath '/user/cloudera/contact.csv' overwrite into table contact;

Step 5: Create a table in MYSQL in which data needs to be exported

CREATE TABLE CONTACT (
      contactid INTEGER NOT NULL ,
      firstname VARCHAR(50),
      lastname  VARCHAR(50),
      email varchar(50)
);

Step 6: Export the data 

sqoop export \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username=retail_dba \
--password=cloudera \
--table CONTACT \
--export-dir /user/hive/warehouse/prateek.db/contact \



