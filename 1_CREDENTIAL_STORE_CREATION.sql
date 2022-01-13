-- Credential Store Creation to access Object Storage from ADW

begin
  dbms_cloud.drop_credential(credential_name => 'OBJ_STORE_GRAPH_LAB_EXTT');
end;
/

BEGIN
DBMS_CLOUD.CREATE_CREDENTIAL(
credential_name => 'OBJ_STORE_GRAPH_LAB_EXTT',
username => 'Your OCI User',
password => 'The created OCI tocken'
);
END;
/

SELECT *
FROM ALL_CREDENTIALS
;

select *
from dbms_cloud.list_objects('OBJ_STORE_GRAPH_LAB_EXTT', 'Your Object Storage bucket url' )
;
