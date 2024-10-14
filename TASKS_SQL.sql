create or replace task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
	schedule='5 minute'
	as COPY INTO pl_game_logs
    FROM @uni_kishore_pipeline
    file_format = (format_name=ff_json_logs);

create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
	schedule = '5 minute'
	as
    MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
    USING (
        SELECT 
        ed_pipeline_logs.ip_address,
        ed_pipeline_logs.user_login as gamer_name,
        ed_pipeline_logs.user_event as game_event_name,
        ed_pipeline_logs.datetime_iso8601 as game_event_utc,
        city,
        region,
        country,
        timezone as gamer_ltz_name,
        CONVERT_TIMEZONE('UTC', timezone, datetime_iso8601) as game_event_ltz,
        DAYNAME(game_event_ltz) as dow_name,
        tod_name
        FROM AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS ed_pipeline_logs
        JOIN IPINFO_GEOLOC.demo.location loc 
        ON IPINFO_GEOLOC.public.TO_JOIN_KEY(ed_pipeline_logs.ip_address) = loc.join_key
        AND IPINFO_GEOLOC.public.TO_INT(ed_pipeline_logs.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN time_of_day_lu
        ON time_of_day_lu.hour = HOUR(game_event_ltz)
            ) r
        ON r.gamer_name = e.gamer_name
        AND r.game_event_utc = e.game_event_utc
        AND r.game_event_name = e.game_event_name
        WHEN NOT MATCHED THEN
        INSERT(
        IP_ADDRESS,
        GAMER_NAME,
        GAME_EVENT_NAME,
        GAME_EVENT_UTC, CITY,
        REGION, COUNTRY,
        GAMER_LTZ_NAME,
        GAME_EVENT_LTZ,
        DOW_NAME, TOD_NAME
        )
        VALUES(
        IP_ADDRESS,
        GAMER_NAME,
        GAME_EVENT_NAME,
        GAME_EVENT_UTC, CITY,
        REGION, COUNTRY,
        GAMER_LTZ_NAME,
        GAME_EVENT_LTZ,
        DOW_NAME,
        TOD_NAME
        );

create or replace task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	SCHEDULE = '5 minutes'
    WHEN
    system$stream_has_data('ed_cdc_stream')
	as 
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);
        
--Resume the task so it is running
alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED resume;