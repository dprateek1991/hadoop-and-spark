/*############################################################
# Author        : Prateek Dubey
#############################################################*/

REGISTER $PIGGYBANK_LIB;
DEFINE DECODE $DECODE_LIB;
SET mapred.fairscheduler.pool $POOL_NAME;

TABLE_A = LOAD '$HIVE_RAW_DB.TABLE_A' USING $HCATLOADER_LIB;

TABLE_A_FILTER = FILTER TABLE_A BY (call_date >= '$FROM_DT' and call_date <= '$THRU_DT');

TABLE_A_SELECT = FOREACH TABLE_A_FILTER GENERATE 
	'Inbound' as alias1,
	call_id as call_id,
	clnt_id as clnt_id,
	TRIM(call_date) as call_date,
	TRIM(call_beg_tm) as call_beg_tm,
	assc_id as assc_id;

TABLE_B = LOAD '$HIVE_RAW_DB.TABLE_B' USING $HCATLOADER_LIB;

TABLE_B_FILTER = FILTER TABLE_B BY (call_date >= '$FROM_DT' and call_date <= '$THRU_DT');

TABLE_B_SELECT = FOREACH TABLE_B_FILTER GENERATE 
	call_id as call_id,
	clnt_id as clnt_id,
	TRIM(call_date) as call_date,
	TRIM(call_beg_tm) as call_beg_tm,
	assc_id as assc_id,
	call_topic_id as call_topic_id,
	TRIM(call_topic_description) as call_topic_description,
	TRIM(call_subtopic_description) as call_subtopic_description;

TABLE_A_B_JOIN = JOIN TABLE_A_SELECT BY (clnt_id,call_id) , TABLE_B_SELECT BY (clnt_id,call_id);

TABLE_A_B_JOIN_RESULT = FOREACH TABLE_A_B_JOIN GENERATE 
	TABLE_A_SELECT::clnt_id as clnt_id,
	TABLE_A_SELECT::alias1 as alias1,
	TABLE_A_SELECT::call_id as call_id,
	TABLE_A_SELECT::call_date as call_date,
	TABLE_A_SELECT::call_beg_tm as call_beg_tm,
	TABLE_A_SELECT::assc_id as assc_id,
	TABLE_B_SELECT::call_topic_id as call_topic_id,
	TABLE_B_SELECT::call_topic_description as call_topic_description,
	TABLE_B_SELECT::call_subtopic_description as call_subtopic_description;

TABLE_C = LOAD '$HIVE_RAW_DB.TABLE_C' USING $HCATLOADER_LIB;

TABLE_C_SELECT = FOREACH TABLE_C GENERATE
    clnt_id as clnt_id;
    
TABLE_A_B_C_JOIN = JOIN TABLE_A_B_JOIN_RESULT BY (clnt_id) , TABLE_C_SELECT BY (clnt_id);

TABLE_A_B_C_JOIN_RESULT = FOREACH TABLE_A_B_C_JOIN GENERATE 
	TABLE_C_SELECT::clnt_id as clnt_id,
	TABLE_A_B_JOIN_RESULT::alias1 as alias1,
	TABLE_A_B_JOIN_RESULT::call_id as call_id,
	TRIM(ToString(ToDate(TABLE_A_B_JOIN_RESULT::call_date,'$DATE_FMT'),'MM/dd/yyyy')) as call_date,
	TABLE_A_B_JOIN_RESULT::call_beg_tm as call_beg_tm,
	TABLE_A_B_JOIN_RESULT::assc_id as assc_id,
	TABLE_A_B_JOIN_RESULT::call_topic_id as call_topic_id,
	TABLE_A_B_JOIN_RESULT::call_topic_description as call_topic_description,
	TABLE_A_B_JOIN_RESULT::call_subtopic_description as call_subtopic_description;

FINAL_RESULT = ORDER TABLE_A_B_C_JOIN_RESULT BY call_date,call_beg_tm,assc_id ASC ;

	rmf $RAW_DIR/FINAL_RESULT.csv;

	STORE FINAL_RESULT INTO '$RAW_DIR/FINAL_RESULT.csv' USING PigStorage('~');
