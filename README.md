# From JSON to Graph in a Box
## Data Lakehouse LAB on Oracle OCI with Objectst Storage and Autonomous Database (ADW)

You can see the problem and LAB description on [this **Medium** articole](https://medium.com/@michelemiscione/from-json-to-graph-in-a-box-aa9cb0866546).

### Dataset

You can downloiad the dataset [on this link](https://www.kaggle.com/sripaadsrinivasan/indian-railways-dataset).
### Files

- 1_CREDENTIAL_STORE_CREATION.sql: sql script to create the credential store to access files on Object storage from the ADW
- 2_STATIONS_ELAB.sql: sql script data flow to elaborate the stations.json file
- 3_SCHEDULES_ELAB.sql: sql script data flow to elaborate the schedules.json file
- 4_PG_VIEW_ENABLING.sql: sql script to enable the PG_VIEW feature in the ADW Graph Studio
- 5_CREATE_PROPERTY_GRAPH.pgx.sql: PGX script to create the property graph, this can be runned on the ADW Graph Studio
- 6_QUERY_PROPERTY_GRAPH.pgql-pgx.sql: some example of PGQL queries
- 7_Indian Train Schedule Exploration LAB.dsnb: ADW Graph Studio notebook export to teploduce all the analisys
