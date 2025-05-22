SHOW CATALOGS;

CREATE SCHEMA datalake.iceberg;
CREATE SCHEMA hive.hive
WITH (location = 's3://datalake/data/logs/hive/');

SHOW tables FROM datalake.iceberg;
SHOW tables FROM hive.hive;

CREATE TABLE datalake.iceberg.request_logs (
  request_time TIMESTAMP,
  Url VARCHAR,
  ip VARCHAR,
  user_agent VARCHAR,
  year VARCHAR,
  month VARCHAR,
  day VARCHAR
)
WITH (
  format = 'PARQUET',
  location = 's3://datalake/data/logs/',
  partitioning = ARRAY['year','month', 'day']
);

CREATE TABLE hive.hive.request_logs (
  request_time TIMESTAMP,
  Url VARCHAR,
  ip VARCHAR,
  user_agent VARCHAR,
  year VARCHAR,
  month VARCHAR,
  day VARCHAR
)
WITH (
  format = 'PARQUET',
  partitioned_by = ARRAY['year','month', 'day']
);

INSERT INTO datalake.iceberg.request_logs VALUES (timestamp '2024-05-01 01:00 UTC', 'http://test1', '0.0.0.0', 'YQ', '2024', '05', '01');
INSERT INTO datalake.iceberg.request_logs VALUES (timestamp '2024-05-01 02:00 UTC', 'http://test2', '0.0.0.0', 'YQ', '2024', '05', '01');
INSERT INTO datalake.iceberg.request_logs VALUES (timestamp '2024-05-01 03:00 UTC', 'http://test3', '0.0.0.0', 'YQ', '2024', '05', '01');
INSERT INTO datalake.iceberg.request_logs VALUES (timestamp '2024-05-02 01:00 UTC', 'http://test1', '0.0.0.0', 'YQ', '2024', '05', '02');
INSERT INTO datalake.iceberg.request_logs VALUES (timestamp '2024-05-03 01:00 UTC', 'http://test2', '0.0.0.0', 'YQ', '2024', '05', '03');
INSERT INTO datalake.iceberg.request_logs VALUES (timestamp '2024-05-04 01:00 UTC', 'http://test1', '0.0.0.0', 'YQ', '2024', '05', '04');

INSERT INTO hive.hive.request_logs VALUES (timestamp '2024-05-01 01:00 UTC', 'http://test1', '0.0.0.0', 'YQ', '2024', '05', '01');
INSERT INTO hive.hive.request_logs VALUES (timestamp '2024-05-01 02:00 UTC', 'http://test2', '0.0.0.0', 'YQ', '2024', '05', '01');
INSERT INTO hive.hive.request_logs VALUES (timestamp '2024-05-01 03:00 UTC', 'http://test3', '0.0.0.0', 'YQ', '2024', '05', '01');
INSERT INTO hive.hive.request_logs VALUES (timestamp '2024-05-02 01:00 UTC', 'http://test1', '0.0.0.0', 'YQ', '2024', '05', '02');
INSERT INTO hive.hive.request_logs VALUES (timestamp '2024-05-03 01:00 UTC', 'http://test2', '0.0.0.0', 'YQ', '2024', '05', '03');
INSERT INTO hive.hive.request_logs VALUES (timestamp '2024-05-04 01:00 UTC', 'http://test1', '0.0.0.0', 'YQ', '2024', '05', '04');

SELECT * FROM datalake.iceberg.request_logs;
SELECT * FROM hive.hive.request_logs;

ANALYZE datalake.iceberg.request_logs;
ANALYZE hive.hive.request_logs;

CREATE SCHEMA datalake.final;
