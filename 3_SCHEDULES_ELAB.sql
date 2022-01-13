-- ------------------------------------------------------------------------------------------------------------------------------------------------
-- Access and prepare schedules.json data
-- ------------------------------------------------------------------------------------------------------------------------------------------------

-- SCHEDULES External Table creation

DROP table EXT_COLL_SCHEDULES purge;

BEGIN
DBMS_CLOUD.CREATE_EXTERNAL_TABLE(
table_name =>'EXT_COLL_SCHEDULES',
credential_name =>'OBJ_STORE_GRAPH_LAB_EXTT',
file_uri_list =>'Your Object Storage bucket url/pi_schedules.json',
format => json_object('readsize' value '90000000', 'recorddelimiter' value 'newline','maxdocsize' value 104857600, 'rejectlimit' value 10000000 , 'unpackarrays' value TRUE ),
column_list => 'json_document blob',
field_list => 'json_document raw'
);
END;
/


BEGIN
   DBMS_CLOUD.VALIDATE_EXTERNAL_TABLE(
      table_name => 'EXT_COLL_SCHEDULES',
      schema_name => 'Your Schema Name'
);
END;
/

-- Alternately copy JSON file from the Object Storage to the ADW as a document in a collection


BEGIN
DBMS_CLOUD.COPY_COLLECTION(
collection_name => 'STG_COLL_SCHEDULES',
credential_name => 'OBJ_STORE_GRAPH_LAB_EXTT',
file_uri_list => 'Your Object Storage bucket url/schedules.json',
format => json_object('recorddelimiter' value 'newline','maxdocsize' value 104857600, 'rejectlimit' value 10000000 , 'unpackarrays' value TRUE )
);
END;
/

-- Test SCHEDULES External Table or Document Collection JSON query (you can use EXT_COLL_SCHEDULES view or STG_COLL_SCHEDULES table)

SELECT to_clob(json_document) AS data FROM EXT_COLL_SCHEDULES;

SELECT
json_query(json_document, '$[1].id' RETURNING clob WITH ARRAY WRAPPER ERROR ON ERROR) AS features
FROM
    EXT_COLL_SCHEDULES
WHERE
    json_document IS JSON;

select t.* 
FROM
    EXT_COLL_SCHEDULES,
    JSON_TABLE ( JSON_DOCUMENT, '$[*]'
            COLUMNS (
                ID NUMBER PATH '$.id',
                TRAIN_NUMBER NUMBER PATH '$.train_number',
                TRAIN_NAME VARCHAR2 ( 1000 ) PATH '$.train_name',
                STATION_NAME VARCHAR2 ( 1000 ) PATH '$.station_name',
                STATION_CODE VARCHAR2 ( 10 ) PATH '$.station_code',
                ARRIVAL VARCHAR2 ( 10 ) PATH '$.arrival',
                DAY NUMBER PATH '$.day',
                DEPARTURE VARCHAR2 ( 10 ) PATH '$.departure'
            )
        )
    AS T
;

-- Create a EDGES view for the Property Graph Creation 

CREATE OR REPLACE VIEW VW_SCHEDULES AS
SELECT
    T."ID",
    T."TRAIN_NUMBER",
    T."TRAIN_NAME",
    T."STATION_NAME",
    T."STATION_CODE",
    T."ARRIVAL",
    T."DAY",
    T."DEPARTURE"
FROM
    EXT_COLL_SCHEDULES,
    JSON_TABLE ( JSON_DOCUMENT, '$[*]'
            COLUMNS (
                ID NUMBER PATH '$.id',
                TRAIN_NUMBER NUMBER PATH '$.train_number',
                TRAIN_NAME VARCHAR2 ( 1000 ) PATH '$.train_name',
                STATION_NAME VARCHAR2 ( 1000 ) PATH '$.station_name',
                STATION_CODE VARCHAR2 ( 10 ) PATH '$.station_code',
                ARRIVAL VARCHAR2 ( 10 ) PATH '$.arrival',
                DAY NUMBER PATH '$.day',
                DEPARTURE VARCHAR2 ( 10 ) PATH '$.departure'
            )
        )
    AS T
WHERE
    T."DAY" IS NOT NULL
    AND T."STATION_CODE" <> 'P';





-- Apply some enrichment and data clinsing to the SCHEDULES to have the EDGES reaty for the Propery Graph creation

CREATE OR REPLACE VIEW VW_HOPS AS
    SELECT
        ID,
        TRAIN_NUMBER,
        TRAIN_NAME,
        DAY,
        DEPARTURE,
        TO_DATE(DEPARTURE, 'HH24:MI:SS') + DAY - 1                                                         DEPARTURE_DATE,
        ( TO_DATE(DEPARTURE, 'HH24:MI:SS') + DAY - 1 - TO_DATE('00:00:00', 'HH24:MI:SS') ) * 24 * 60       DEP_MINUTES,
        STATION_CODE,
        STATION_NAME,
        NEW_ARRIVAL,
        NEW_DAY,
        TO_DATE(NEW_ARRIVAL, 'HH24:MI:SS') + NEW_DAY - 1                                                   NEW_ARRIVAL_DATE,
        ( TO_DATE(NEW_ARRIVAL, 'HH24:MI:SS') + NEW_DAY - 1 - TO_DATE('00:00:00', 'HH24:MI:SS') ) * 24 * 60 ARR_MINUTES,
        STATION_ARRIVAL_CODE,
        STATION_ARRIVAL_NAME,
        CASE
            WHEN ( ( ( TO_DATE(NEW_ARRIVAL, 'HH24:MI:SS') + NEW_DAY - 1 - TO_DATE('00:00:00', 'HH24:MI:SS') ) * 24 * 60 - ( TO_DATE(DEPARTURE,
            'HH24:MI:SS') + DAY - 1 - TO_DATE('00:00:00', 'HH24:MI:SS') ) * 24 * 60 ) * 60 ) = 0 THEN
                0.1
            ELSE
                ( ( ( TO_DATE(NEW_ARRIVAL, 'HH24:MI:SS') + NEW_DAY - 1 - TO_DATE('00:00:00', 'HH24:MI:SS') ) * 24 * 60 - ( TO_DATE(DEPARTURE,
                'HH24:MI:SS') + DAY - 1 - TO_DATE('00:00:00', 'HH24:MI:SS') ) * 24 * 60 ) * 60 )
        END                                                                                                DURATION_SEC
    FROM
        (
            SELECT
                ID,
                TRAIN_NUMBER,
                TRAIN_NAME,
                DAY,
                DEPARTURE,
                STATION_CODE,
                STATION_NAME,
                NEW_ARRIVAL,
                NEW_DAY,
                STATION_ARRIVAL_CODE,
                STATION_ARRIVAL_NAME
            FROM
                (
                    SELECT
                        ID,
                        TRAIN_NUMBER,
                        TRAIN_NAME,
                        DAY,
                        DEPARTURE,
                        STATION_CODE,
                        STATION_NAME,
                        CASE
                            WHEN LEAD(ARRIVAL, 1, 0)
                                 OVER(PARTITION BY TRAIN_NUMBER
                                      ORDER BY
                                          ID
                                 ) IN ( '0', 'None' ) THEN
                                DEPARTURE
                            ELSE
                                LEAD(ARRIVAL, 1, 0)
                                OVER(PARTITION BY TRAIN_NUMBER
                                     ORDER BY
                                         ID
                                )
                        END NEW_ARRIVAL,
                        CASE
                            WHEN LEAD(DAY, 1, 0)
                                 OVER(PARTITION BY TRAIN_NUMBER
                                      ORDER BY
                                          ID
                                 ) = 0 THEN
                                DAY
                            ELSE
                                LEAD(DAY, 1, 0)
                                OVER(PARTITION BY TRAIN_NUMBER
                                     ORDER BY
                                         ID
                                )
                        END NEW_DAY,
                        LEAD(STATION_CODE, 1, 0)
                        OVER(PARTITION BY TRAIN_NUMBER
                             ORDER BY
                                 ID
                        )   STATION_ARRIVAL_CODE,
                        LEAD(STATION_NAME, 1, 0)
                        OVER(PARTITION BY TRAIN_NUMBER
                             ORDER BY
                                 ID
                        )   STATION_ARRIVAL_NAME
                    FROM
                        VW_SCHEDULES_NO_NULL
                    WHERE
                        DEPARTURE <> 'None'
                    ORDER BY
                        ID ASC
                )
        )
    WHERE
            NEW_ARRIVAL <> '0'
        AND STATION_ARRIVAL_CODE <> '0'
        AND ( TO_DATE(NEW_ARRIVAL, 'HH24:MI:SS') + NEW_DAY - 1 - TO_DATE('00:00:00', 'HH24:MI:SS') ) * 24 * 60 - ( TO_DATE(DEPARTURE,
        'HH24:MI:SS') + DAY - 1 - TO_DATE('00:00:00', 'HH24:MI:SS') ) * 24 * 60 >= 0;

