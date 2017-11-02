#####################################################################################
# Script Name   : impala_ddl_creation  
# Author        : Prateek Dubey 
# Purpose       : Script to generate hive ddls using Impala
#####################################################################################

#!/usr/bin/ksh

## Define global variables

. prateek.sh

## Define local variables

PROCESS_NM="generate_hive_ddl"
TS=`date '+%Y%m%d%H%M%S'`

## Checking user defined options

while getopts :d:t: args
do
  case $args in
   d)HIVE_DB=$OPTARG;;
   t)HIVE_DB_TRGT=$OPTARG;;  
   ?)echo "ERROR : Usage --> `basename $0` -d HIVE_DB -t HIVE_DB_TRGT" && exit 1;;
  esac
done

echo "Process STARTED at `date '+%m-%d-%Y %H:%M:%S'`"

## If a valid hive schema is not provided then error out

if [[ ${HIVE_DB} = "" ]] ; then

     echo "ERROR : Please provide a valid hive database name using -d option" && exit 2

fi ;

## Print input variables

echo "HIVE_DB=${HIVE_DB}"

     hive -S -e "USE ${HIVE_DB};SHOW TABLES" > ${LOG_DIR}/${PROCESS_NM}.${HIVE_DB}.${TS}.tbl
	 
     if [[ $? -ne 0 ]] ; then
	 
          echo "ERROR : Hive process failed while executing command \"USE ${HIVE_DB};SHOW TABLES\"" && exit 3
		  
     fi ;
	 
     for i in `cat ${LOG_DIR}/${PROCESS_NM}.${HIVE_DB}.${TS}.tbl`
     do
      ##  hive -S -e "SHOW CREATE TABLE ${HIVE_DB}.${i}" > ${LOG_DIR}/${PROCESS_NM}.${HIVE_DB}.ddl.$$	

	${CONNECT_IMPALA} -B -q "SHOW CREATE TABLE ${HIVE_DB}.${i};" 2>/dev/null|nawk 'NR>=1' >  ${LOG_DIR}/${PROCESS_NM}.${HIVE_DB}.ddl.$$  
		
        if [[ $? -ne 0 ]] ; then
	 
             echo "ERROR : Impala process failed while executing command \"SHOW CREATE TABLE ${HIVE_DB}.${i}\"" && exit 4
		  
        fi ;
	
        line_num=`grep -n 'LOCATION' ${LOG_DIR}/${PROCESS_NM}.${HIVE_DB}.ddl.$$|awk -F":" '{print $1}'` 

        let line_num-=1

        sed -n '/CREATE/,'${line_num}'p' ${LOG_DIR}/${PROCESS_NM}.${HIVE_DB}.ddl.$$ > ${LOG_DIR}/${PROCESS_NM}.${HIVE_DB}.ddl.$$.tmp
	
	sed '/SERDEPROPERTIES/d' ${LOG_DIR}/${PROCESS_NM}.${HIVE_DB}.ddl.$$.tmp > ${LOG_DIR}/${PROCESS_NM}.${HIVE_DB}.ddl.$$.tmp1

	sed 's/"//g' ${LOG_DIR}/${PROCESS_NM}.${HIVE_DB}.ddl.$$.tmp1 > ${LOG_DIR}/${PROCESS_NM}.${HIVE_DB}.ddl.$$.tmp2

        nawk -v cnt=`sed -n '$=' ${LOG_DIR}/${PROCESS_NM}.${HIVE_DB}.ddl.$$.tmp2` '{if(NR==cnt) 
                                                                                      print $0";" ; 
                                                                                   else 
                                                                                      print $0
                                                                                  }' ${LOG_DIR}/${PROCESS_NM}.${HIVE_DB}.ddl.$$.tmp2 > ${LOG_DIR}/${PROCESS_NM}.${HIVE_DB}.ddl.$$.tmp3

        grep -v '^COMMENT.*Imported by sqoop' ${LOG_DIR}/${PROCESS_NM}.${HIVE_DB}.ddl.$$.tmp3 > ${LOG_DIR}/${PROCESS_NM}.${HIVE_DB}.ddl.$$.tmp4

cat <<! >> ${CONFIG_DIR}/${PROCESS_NM}.${HIVE_DB}.${TS}.ddl
---------------------------------------- ${HIVE_DB}.${i} -----------------------------------------------

`cat ${LOG_DIR}/${PROCESS_NM}.${HIVE_DB}.ddl.$$.tmp4`

--------------------------------------------------------------------------------------------------------
!

     done

sed 's/\\u0001/\\001/' ${CONFIG_DIR}/${PROCESS_NM}.${HIVE_DB}.${TS}.ddl > ${CONFIG_DIR}/${PROCESS_NM}.${HIVE_DB}.${TS}.ddl.tmp

mv ${CONFIG_DIR}/${PROCESS_NM}.${HIVE_DB}.${TS}.ddl.tmp ${CONFIG_DIR}/${PROCESS_NM}.${HIVE_DB}.${TS}.ddl

cp ${CONFIG_DIR}/${PROCESS_NM}.${HIVE_DB}.${TS}.ddl ${CONFIG_DIR}/${HIVE_DB}.ddl 

if [[ ${HIVE_DB_TRGT} = "" ]] ; then

     echo "No target database name passed, keeping it as same" 
     
else

echo $HIVE_DB
echo $HIVE_DB_TRGT

echo "perl -i -pe 's/$HIVE_DB/$HIVE_DB_TRGT/g' ${CONFIG_DIR}/${HIVE_DB}.ddl" 

perl -i -pe "s/$HIVE_DB/$HIVE_DB_TRGT/g" ${CONFIG_DIR}/${HIVE_DB}.ddl

fi ;

## Remove temporary files

rm -f ${LOG_DIR}/${PROCESS_NM}.${HIVE_DB}*

echo "DDL file created as : ${CONFIG_DIR}/${PROCESS_NM}.${HIVE_DB}.${TS}.ddl"

echo "SUCCESS : Process ENDED at `date '+%m-%d-%Y %H:%M:%S'`"
