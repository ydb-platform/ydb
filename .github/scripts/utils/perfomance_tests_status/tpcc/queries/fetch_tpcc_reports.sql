-- Allure report URLs for TPC-C suites (joined onto perfomance/tpcc points in generate.py).
-- Source: tests_results (Suite TpccW* / Test=test); mart perfomance/tpcc has no report_url.
-- Replace {{SINCE}} with ISO timestamp = today − window_days (report_config.json).
SELECT
  Suite,
  Test,
  JSON_VALUE(Info, '$.ci_cluster_name') AS ci_cluster_name,
  JSON_VALUE(Info, '$.report_url') AS report_url,
  Timestamp AS timestamp
FROM `perfomance/olap/tests_results`
WHERE Timestamp >= Timestamp('{{SINCE}}')
  AND StartsWith(Suite, 'TpccW')
  AND Test = 'test'
  AND JSON_VALUE(Info, '$.report_url') IS NOT NULL
ORDER BY Timestamp;
