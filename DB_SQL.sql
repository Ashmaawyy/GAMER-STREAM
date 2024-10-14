create or replace file format FF_json_logs
type = 'JSON' 
compression = 'AUTO' 
enable_octal = FALSE
allow_duplicate = FALSE 
strip_outer_array = TRUE
strip_null_values = FALSE 
ignore_utf8_errors = FALSE;

select $1 from @uni_kishore/kickoff (file_format => ff_json_logs);

copy into game_logs
from @uni_kishore/kickoff
file_format = (format_name=ff_json_logs);

create or replace view LOGS as
(select
--RAW_LOG:agent::text as AGENT,
RAW_LOG:user_event::text as USER_EVENT,
RAW_LOG:datetime_iso8601::timestamp_ntz as DATETIME_ISO8601,
RAW_LOG:user_login::text as USER_LOGIN,
RAW_LOG:ip_address::text as IP_ADDRESS,
*
FROM GAME_LOGS WHERE RAW_LOG:ip_address::text is not null);

select * from logs;

copy into game_logs
from @uni_kishore/updated_feed
file_format = (format_name=ff_json_logs);

create table ags_game_audience.raw.time_of_day_lu
(  hour number
   ,tod_name varchar(25)
);

insert into time_of_day_lu
values
(6,'Early morning'),
(7,'Early morning'),
(8,'Early morning'),
(9,'Mid-morning'),
(10,'Mid-morning'),
(11,'Late morning'),
(12,'Late morning'),
(13,'Early afternoon'),
(14,'Early afternoon'),
(15,'Mid-afternoon'),
(16,'Mid-afternoon'),
(17,'Late afternoon'),
(18,'Late afternoon'),
(19,'Early evening'),
(20,'Early evening'),
(21,'Late evening'),
(22,'Late evening'),
(23,'Late evening'),
(0,'Late at night'),
(1,'Late at night'),
(2,'Late at night'),
(3,'Toward morning'),
(4,'Toward morning'),
(5,'Toward morning');

select tod_name, listagg(hour,',') 
from time_of_day_lu
group by tod_name;

create or replace table ags_game_audience.enhanced.logs_enhanced as(
--Use two functions supplied by IPShare to help with an efficient IP Lookup Process!
SELECT logs.ip_address
, logs.user_login as gamer_name
, logs.user_event as game_event_name
, logs.datetime_iso8601 as game_event_utc
, city
, region
, country
, timezone as gamer_ltz_name
, CONVERT_TIMEZONE('UTC', timezone, datetime_iso8601) as game_event_ltz
, DAYNAME(game_event_ltz) as dow_name
, tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN time_of_day_lu
ON time_of_day_lu.hour = HOUR(game_event_ltz)
);

use role accountadmin;
--You have to run this grant or you won't be able to test your tasks while in SYSADMIN role
--this is true even if SYSADMIN owns the task!!
grant execute task on account to role SYSADMIN;

use role sysadmin; 

--Now you should be able to run the task, even if your role is set to SYSADMIN
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--the SHOW command might come in handy to look at the task 
show tasks in account;

--you can also look at any task more in depth using DESCRIBE
describe task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--clone the table to save this version as a backup
--since it holds the records from the UPDATED FEED file, we'll name it _UF
create or replace table ags_game_audience.enhanced.LOGS_ENHANCED_UF 
clone ags_game_audience.enhanced.LOGS_ENHANCED;

--let's truncate so we can start the load over again
truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

select * from logs_enhanced;

create or replace TABLE AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS (
	RAW_LOG VARIANT
);

execute task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES;

create or replace view PL_LOGS as
(select
--RAW_LOG:agent::text as AGENT,
RAW_LOG:user_event::text as USER_EVENT,
RAW_LOG:datetime_iso8601::timestamp_ntz as DATETIME_ISO8601,
RAW_LOG:user_login::text as USER_LOGIN,
RAW_LOG:ip_address::text as IP_ADDRESS,
*
FROM PL_GAME_LOGS WHERE RAW_LOG:ip_address::text is not null);

-- TESTING THE PIPELINE--

--Step 1 - how many files in the bucket?
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;

--Step 2 - number of rows in raw table (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;

--Step 3 - number of rows in raw view (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_LOGS;

--Step 4 - number of rows in enhanced table (should be file count x 10 but fewer rows is okay because not all IP addresses are available from the IPInfo share)
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

use role accountadmin;
grant EXECUTE MANAGED TASK on account to SYSADMIN;

--switch back to sysadmin
use role sysadmin;

CREATE OR REPLACE TABLE ED_PIPELINE_LOGS
AS(
 SELECT 
    METADATA$FILENAME as log_file_name --new metadata column
  , METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
  , current_timestamp(0) as load_ltz --new local time of load
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
  (file_format => 'ff_json_logs')
  );

TRUNCATE TABLE ED_PIPELINE_LOGS;

CREATE OR REPLACE PIPE PIPE_GET_NEW_FILES
auto_ingest=true
aws_sns_topic='arn:aws:sns:us-west-2:321463406630:dngw_topic'
AS 
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name 
  , METADATA$FILE_ROW_NUMBER as log_file_row_id 
  , current_timestamp(0) as load_ltz 
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);

select parse_json(SYSTEM$PIPE_STATUS( 'ags_game_audience.raw.PIPE_GET_NEW_FILES' ));

--create a stream that will keep track of changes to the table
create or replace stream ags_game_audience.raw.ed_cdc_stream 
on table AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

select * 
from ags_game_audience.raw.ed_cdc_stream;

alter pipe pipe_get_new_files set pipe_execution_paused = true;