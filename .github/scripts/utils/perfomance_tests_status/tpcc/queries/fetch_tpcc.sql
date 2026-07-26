-- TPC-C runs for the Now report window (see report_config.json window_days).
-- Replace {{SINCE}} with ISO timestamp = today − window_days, e.g. 2026-05-27T00:00:00Z
SELECT
  cluster,
  run_type,
  warehouses,
  COALESCE(git_branch, '') AS git_branch,
  timestamp,
  git_commit_timestamp,
  tpmC,
  newOrderLatency90 AS lat90,
  efficiency,
  version
FROM `perfomance/tpcc`
WHERE timestamp >= Timestamp('{{SINCE}}')
  AND run_type LIKE 'ydb_cli_%'
ORDER BY cluster, run_type, warehouses, git_branch, timestamp;
