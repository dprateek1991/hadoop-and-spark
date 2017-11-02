#####################################################################################
# Script Name   : hive_table_creation  
# Author        : Prateek Dubey 
# Purpose       : Script to generate hive tables 
#####################################################################################

#!/usr/bin/ksh

## Define global variables

. prateek.sh

## Define local variables

PROCESS_NM="generate_hive_ddl"
TS=`date '+%Y%m%d%H%M%S'`

## Checking user defined options

while getopts :f: args
do
  case $args in
   f)FILE_NM=$OPTARG;;	
   ?)echo "ERROR : Usage --> `basename $0`" && exit 1;;
  esac
done

echo "Process STARTED at `date '+%m-%d-%Y %H:%M:%S'`"

if [[ ! -f $CONFIG_DIR/$FILE_NM.ddl ]]; then
 
	echo "ERROR: Please provide a valid filename, as it doesn't exist" && exit 2

fi ; 	

beeline -n "${USER}" -p "x" --silent=false --verbose=true --showNestedErrs=true --fastConnect=true -u "'${CONNECT_HIVE}'" -f $CONFIG_DIR/$FILE_NM.ddl 

if [[ $? -eq 0 ]] ; then

echo "####################################################################################################"
echo "Table creation completed"
echo "####################################################################################################"
else
echo "####################################################################################################"
echo "Table creation failed" && exit 99
echo "####################################################################################################"

fi;

echo "SUCCESS : Process ENDED at `date '+%m-%d-%Y %H:%M:%S'`"
