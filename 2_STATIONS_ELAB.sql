-- ------------------------------------------------------------------------------------------------------------------------------------------------
-- Access and prepare stations.json data
-- ------------------------------------------------------------------------------------------------------------------------------------------------

-- STATIONS External Table creation

DROP table EXT_COLL_STATION purge;

BEGIN
   DBMS_CLOUD.CREATE_EXTERNAL_TABLE(
    table_name =>'EXT_COLL_STATION',
    credential_name =>'OBJ_STORE_GRAPH_LAB_EXTT',
    file_uri_list =>'Your Object Storage bucket url/stations.json',
    format => json_object('recorddelimiter' value 'newline','maxdocsize' value 104857600, 'rejectlimit' value 10000000 , 'unpackarrays' value TRUE ), 
    column_list => 'json_document blob',
    field_list => 'json_document char(5000000000)'
     );
END;
/

BEGIN
   DBMS_CLOUD.VALIDATE_EXTERNAL_TABLE(
      table_name => 'EXT_COLL_STATION',
      schema_name => 'Your Schema Name'
);
END;
/

-- Alternately copy JSON file from the Object Storage to the ADW as a document in a collection

BEGIN
DBMS_CLOUD.COPY_COLLECTION(
collection_name => 'STG_COLL_STATION',
credential_name => 'OBJ_STORE_GRAPH_LAB_EXTT',
file_uri_list => 'Your Object Storage bucket url/stations.json',
format => json_object('recorddelimiter' value 'newline','maxdocsize' value 104857600, 'rejectlimit' value 10000000 , 'unpackarrays' value TRUE )
);
END;
/


-- Test STATIONS External Table or Document Collection JSON query (you can use EXT_COLL_STATION view or STG_COLL_STATION table)

SELECT to_clob(json_document) AS data FROM EXT_COLL_STATION;

SELECT json_value(json_document, '$.type') AS data FROM EXT_COLL_STATION;

SELECT json_value(json_document, '$.features[1].properties.code' RETURNING CLOB ERROR ON ERROR) AS STATION_CODE FROM EXT_COLL_STATION;

SELECT
    JSON_VALUE(json_document, '$.type')                                       AS type,
    json_query(json_document, '$.features' RETURNING clob WITH ARRAY WRAPPER) AS features
FROM
    ext_coll_station
WHERE
    json_document IS JSON;


-- Prepare VERTEX table for Property Graph by extracting relevant information from JSON file on the Object Storage by Dot Notation

SELECT
    t.*
FROM
    ext_coll_station,
    JSON_TABLE ( json_document FORMAT JSON, '$.features[*]'
            COLUMNS (
                stations_code VARCHAR2 ( 10 ) PATH '$.properties.code',
                stations_address VARCHAR2 ( 2000 ) PATH '$.properties.address',
                stations_name VARCHAR2 ( 100 ) PATH '$.properties.name',
                stations_lon NUMBER PATH '$.geometry.coordinates[0]',
                stations_lat NUMBER PATH '$.geometry.coordinates[1]'
            )
        )
    AS t;

-- Create a VERTEX view for the Property Graph Creation 

CREATE OR REPLACE VIEW VW_STATIONS_ON_EXTJFILE as    
SELECT
    t."STATIONS_CODE",
    t."STATIONS_ADDRESS",
    t."STATIONS_NAME",
    t."STATIONS_LON",
    t."STATIONS_LAT"
FROM
    ext_coll_station,
    JSON_TABLE ( json_document FORMAT JSON, '$.features[*]'
            COLUMNS (
                stations_code VARCHAR2 ( 10 ) PATH '$.properties.code',
                stations_address VARCHAR2 ( 2000 ) PATH '$.properties.address',
                stations_name VARCHAR2 ( 100 ) PATH '$.properties.name',
                stations_lon NUMBER PATH '$.geometry.coordinates[0]',
                stations_lat NUMBER PATH '$.geometry.coordinates[1]'
            )
        )
    AS t
WHERE
    t.stations_code IS NOT NULL
    AND t.stations_name IS NOT NULL;