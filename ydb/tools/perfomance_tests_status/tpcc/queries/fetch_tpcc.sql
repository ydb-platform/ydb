-- TPC-C runs for the Now report window (~1 month by default).
-- Replace {{SINCE}} with ISO timestamp = today − 30d, e.g. 2026-06-25T00:00:00Z
SELECT
  cluster,
  run_type,
  warehouses,
  COALESCE(git_branch, '') AS git_branch,
  timestamp,
  tpmC,
  newOrderLatency90 AS lat90,
  efficiency,
  version
FROM `perfomance/tpcc`
WHERE timestamp >= Timestamp('{{SINCE}}')
  AND run_type LIKE 'ydb_cli_%'
ORDER BY cluster, run_type, warehouses, git_branch, timestamp;
