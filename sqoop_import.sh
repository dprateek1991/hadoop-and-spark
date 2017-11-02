#####################################################################################
# Script Name   : sqoop_import.sh
# Purpose       : Script to import Data from RDBMS into HDFS
#####################################################################################

## Calling global parameter file

#!/usr/bin/ksh

. prateek.sh

## Defining local parameters

PROCESS_NM=`basename ${0%.*}`

while read i

do


HIVE_TBL_NM=`echo $i|cut -d '~' -f1`
DB2_TBL_NM=`echo $i|cut -d '~' -f2`
DB2_SCH_NM=`echo $i|cut -d '~' -f3`
col_list=`echo $i|cut -d '~' -f4`


ALTER_CLS="TRUNCATE TABLE ${HIVE_TBL_NM}"

echo "##############################################"
echo "Sqoop starting for table $HIVE_TBL_NM"
echo "##############################################"

JOB_START_TS=`date '+%Y-%m-%d-%H:%M:%S'`

## Checking if target-dir already exists, if it does then deleting it

JOB_START_TS=`date '+%Y-%m-%d-%H:%M:%S'`

if [[ -z $HIVE_TBL_NM ]] ; then

echo "please pass table name"
exit 1000;
fi ;


hadoop fs -test -e ${HIVE_RAW_DB_DIR}/${HIVE_TBL_NM}/* 2>/dev/null

if [[ $? -eq 0 ]] ; then

     echo "####################################################################"
     echo "INFO : Truncating table ${HIVE_RAW_DB}.${HIVE_TBL_NM}"
     echo "####################################################################"

setsid beeline -n "${USER}" -p "x" --silent=false --verbose=true --showNestedErrs=true --fastConnect=true -u "'${CONNECT_HIVE}'" <<EOT >> $LOG_DIR/${PROCESS_NM}.$$ 2>&1
use ${HIVE_RAW_DB};
${ALTER_CLS};
EOT


     egrep -q -i 'ERROR|FAILED' $LOG_DIR/${PROCESS_NM}.$$


     if [[ $? -eq 0 ]] ; then

          echo "########################################################################################################"
          echo "INFO : Process failed while importing ${RAW_DIR}/${HIVE_TBL_NM} table at `date '+%Y-%m-%d-%H:%M:%S'`"
          echo "########################################################################################################"

          echo "Process failed while importing ${RAW_DIR}/${HIVE_TBL_NM} table at `date '+%Y-%m-%d-%H:%M:%S'`"

     fi ;

fi ;

## Running sqoop import command 

sqoop import ${SQOOP_COMPRESS_SNAPPY} $HDCONNECT \
--query "SELECT ${col_list} FROM ${DB2_SCH_NM}.${DB2_TBL_NM} WHERE \$CONDITIONS" \
--hcatalog-database ${HIVE_RAW_DB} --hcatalog-table ${HIVE_TBL_NM} \
--outdir ${LOG_DIR} \
--null-string '\\N' --null-non-string '' \
-m 1

if [[ $? -ne 0 ]] ; then

     echo "#############################################################################################################"
     echo "ERROR: Sqoop import for ${HIVE_RAW_DB}.${HIVE_TBL_NM} failed at `date '+%Y-%m-%d-%H:%M:%S'`"
     echo "#############################################################################################################"

    status_entry "${PROCESS_NM}.sh" "Sqoop Import" "${HIVE_RAW_DB}.${HIVE_TBL_NM}" "0" "${JOB_START_TS}" "F"
     exit 1000;

else

     echo "#############################################################################################################"
     echo "SUCCESS: Sqoop import for ${HIVE_RAW_DB}.${HIVE_TBL_NM} succeeded at `date '+%Y-%m-%d-%H:%M:%S'`"
     echo "#############################################################################################################"

     status_entry "${PROCESS_NM}.sh" "Sqoop Import" "${HIVE_RAW_DB}.${HIVE_TBL_NM}" "0" "${JOB_START_TS}" "C" 

${CONNECT_IMPALA} -B -q "INVALIDATE METADATA $HIVE_RAW_DB.$HIVE_TBL_NM;"

fi ;

echo "########################################################################################################"
echo "INFO : Sqoop import ended for table $HIVE_TBL_NM at `date '+%Y-%m-%d-%H:%M:%S'`"
echo "########################################################################################################"

done < ${CONFIG_DIR}/${PROCESS_NM}.param
