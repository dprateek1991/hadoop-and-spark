/*############################################################
# Author        : Prateek Dubey
#############################################################*/

REGISTER $PIGGYBANK_LIB;
DEFINE DECODE $DECODE_LIB;
SET mapred.fairscheduler.pool $POOL_NAME;

SOURCE_FILE = LOAD '$RAW_DIR/input_file.csv' using PigStorage('~') as (
	upd:chararray,
	client_id:int,
	job_id:chararray,
	schedule_date:chararray,
	schedule_time:chararray,
	type_code:chararray,
	time_code:chararray,
	time_code_new:chararray,
	client_comments:chararray);

SOURCE = FOREACH SOURCE_FILE GENERATE 
	TRIM(upd) as upd,
	client_id as client_id,
	TRIM(job_id) as job_id,
	ToString(ToDate(TRIM(schedule_date), 'MM/dd/yyyy'),'yyyy-MM-dd') as schedule_date,
	TRIM(schedule_time) as schedule_time,
	TRIM(type_code) as type_code,
	TRIM(time_code) as time_code,
	TRIM(time_code_new) as time_code_new,
	TRIM(client_comments) as client_comments;

SOURCE_FILTER = FILTER SOURCE BY (upd == '01-update' and time_code_new == 'Y');

SOURCE_FILTER_BEFORE_DISTINCT = FOREACH SOURCE_FILTER GENERATE 
	job_id as job_id,
	schedule_date as schedule_date,
	time_code_new as time_code_new,
	client_comments as client_comments;

SOURCE_DISTINCT = DISTINCT SOURCE_FILTER_BEFORE_DISTINCT;

TARGET_TABLE = LOAD '$HIVE_RAW_DB.TARGET_TABLE' USING $HCATLOADER_LIB;

SOURCE_TARGET_JOIN = JOIN TARGET_TABLE BY (schedule_date,job_id) LEFT OUTER,SOURCE_DISTINCT BY (schedule_date,job_id);

FINAL_RESULT = FOREACH SOURCE_TARGET_JOIN  GENERATE
        TARGET_TABLE::client_id as client_id,
        TARGET_TABLE::schedule_date as schedule_date,
        TRIM(TARGET_TABLE::job_id) as job_id,
        TRIM(TARGET_TABLE::type_code) as type_code,
        TARGET_TABLE::type_short_desc as type_short_desc,	
        TARGET_TABLE::type_long_desc as type_long_desc,
		TARGET_TABLE::schedule_time as schedule_time,
		TRIM(TARGET_TABLE::run_date) as run_date,
        TRIM(TARGET_TABLE::run_time) as run_time,
      	(SOURCE_DISTINCT::time_code_new IS NULL ? TRIM(TARGET_TABLE::time_code) :TRIM(SOURCE_DISTINCT::time_code_new)) AS time_code,
		TARGET_TABLE::source_code as source_code,
      	TARGET_TABLE::load_date as load_date,
       	TARGET_TABLE::mm_id as mm_id,
        TARGET_TABLE::yy_id as yy_id,
        TARGET_TABLE::yy_mm_id as yy_mm_id,
        (SOURCE_DISTINCT::client_comments IS NULL ? TARGET_TABLE::client_comments :SOURCE_DISTINCT::client_comments) AS client_comments;

rmf $RAW_DIR/result;

STORE FINAL_RESULT INTO '$RAW_DIR/result' USING PigStorage('$DEF_DELIM');
