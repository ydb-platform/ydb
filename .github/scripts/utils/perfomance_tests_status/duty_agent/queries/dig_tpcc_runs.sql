-- Neighbor TPC-C runs (same source as Now report).
-- Built by tools/dig_runs.py — default: ~35d, same branch, all ydb_cli_* + all clusters.
-- Placeholders shown for documentation only.
SELECT
  cluster,
  run_type,
  warehouses,
  COALESCE(CAST(git_branch AS String), '') AS git_branch,
  timestamp,
  git_commit_timestamp,
  tpmC,
  newOrderLatency90 AS lat90,
  efficiency,
  version
FROM `perfomance/tpcc`
WHERE timestamp >= Timestamp('{{SINCE}}')
  AND timestamp <= Timestamp('{{UNTIL}}')
  AND run_type LIKE 'ydb_cli_%'
  AND (
    git_branch = '{{BRANCH}}'
    OR git_branch = 'origin/{{BRANCH}}'
    OR EndsWith(CAST(git_branch AS String), '/{{BRANCH}}')
  )
ORDER BY cluster, run_type, warehouses, timestamp;
