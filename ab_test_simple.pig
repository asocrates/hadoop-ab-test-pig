/*
    Params
    ------
    statsday = stats day (also day of messages, and active user file)

*/

SET pig.name 'Process aws flume message stream .... ';

SET default_parallel 10;

REGISTER s3://voxer-analytics/pig/external-udfs/Pigitos-1.0-SNAPSHOT.jar;
REGISTER s3://voxer-analytics/pig/external-udfs/datafu-0.0.5.jar;
REGISTER s3://voxer-analytics/pig/external-udfs/json-simple-1.1.jar;
REGISTER s3://voxer-analytics/pig/external-udfs/guava-11.0.2.jar;
REGISTER s3://voxer-analytics/pig/external-udfs/elephant-bird-core-3.0.0.jar;
REGISTER s3://voxer-analytics/pig/external-udfs/elephant-bird-pig-3.0.0.jar;
REGISTER s3://voxer-analytics/pig/external-udfs/oink.jar;

DEFINE jsonStorage com.voxer.data.pig.store.JsonStorage();
DEFINE timestampToDay com.voxer.data.pig.eval.TimestampConverter('yyyyMMdd');
DEFINE timestampToDOW com.voxer.data.pig.eval.TimestampConverter('E');
DEFINE timestampToHOD com.voxer.data.pig.eval.TimestampConverter('H');
DEFINE domainFilter com.voxer.data.pig.eval.EmailDomainFilter();
DEFINE datestampToMillis com.voxer.data.pig.eval.DatestampConverter('yyyyMMdd','GMT');

DEFINE DistinctBy datafu.pig.bags.DistinctBy('0');

IMPORT 's3://voxer-analytics/pig/macros/aws-load-profiles-raw.pig';
IMPORT 's3://voxer-analytics/pig/macros/aws-load-active-users.pig';





invite_links = LOAD '$s3data/invites/$statsday' USING PigStorage() AS (suggested_user_id:chararray, posted_time: double, event:chararray, sms_copy:chararray, pathname:chararray);

invite_create = FILTER invite_links BY event == 'invite_link_create';

invite_click = FILTER invite_links BY event == 'invite_link_click';

temp_join = JOIN invite_create BY suggested_user_id, invite_click BY suggested_user_id;

create_click = FOREACH temp_join GENERATE invite_click::suggested_user_id AS suggested_user_id:chararray,
                                        invite_click::posted_time AS posted_time:double,
                                        invite_click::sms_copy AS sms_copy:chararray,
                                        invite_click::pathname AS pathname:chararray;

/*
summary aggregrates
*/

invite_create_grp_all = GROUP invite_create ALL;

invite_create_cnt = FOREACH invite_create_grp_all GENERATE (long) COUNT(invite_create) AS create_count;

invite_click_grp_all = GROUP invite_click ALL;

invite_click_cnt = FOREACH invite_click_grp_all GENERATE (long) COUNT(invite_click) AS click_count;

create_click_grp_all = GROUP create_click ALL;

create_click_cnt = FOREACH create_click_grp_all GENERATE(long) COUNT(create_click) AS create_click_count;

summary = CROSS invite_create_cnt, invite_click_cnt, create_click_cnt;


/*
group the creates
*/


invite_create = FOREACH invite_create GENERATE suggested_user_id AS suggested_user_id:chararray,
                                                posted_time AS posted_time:double,
                                                sms_copy AS sms_copy:chararray,
                                                pathname AS pathname:chararray;

invite_create_grp_AB_copy = GROUP invite_create BY sms_copy;

invite_create_AB_copy_cnt = FOREACH invite_create_grp_AB_copy GENERATE group AS sms_copy, COUNT(invite_create);

/*
now, group by sms_copy
*/

create_click_grp_AB_copy = GROUP create_click BY sms_copy;

--ILLUSTRATE create_click_grp_AB_copy;

create_click_AB_copy_cnt = FOREACH create_click_grp_AB_copy GENERATE group AS sms_copy, COUNT(create_click);

-- store summary, invite_create_AB_copy_cnt and create_click_AB_copy_cnt

STORE summary INTO '$s3dailystats/$statsday.invites.summary' USING PigStorage();

STORE invite_create_AB_copy_cnt INTO '$s3dailystats/$statsday.invites.create' USING PigStorage();

STORE create_click_AB_copy_cnt INTO '$s3dailystats/$statsday.invites.click' USING PigStorage();









