-- Enabling PG_VIEW Graph Serverver

CREATE TABLE graph.ADB_GRAPH_SETTINGS$ (
  key VARCHAR2(255) primary key,
  value VARCHAR2(255) NOT NULL
);
INSERT INTO graph.ADB_GRAPH_SETTINGS$ VALUES ('PG_VIEWS_ENABLED', 'true');
COMMIT;