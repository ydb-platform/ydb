-- Neighbor OLAP suite runs (same mart as Now report).
-- Built by tools/dig_runs.py — default: ~35d, same branch, related Suite families + all DbAlias.
SELECT
  Branch,
  Version,
  DbAlias,
  Suite,
  RunTs,
  YdbSumMeans,
  GrossTime,
  SuccessCount,
  FailCount,
  FailTests,
  Report
FROM `perfomance/olap/fast_results_siutes`
WHERE RunTs >= Timestamp('{{SINCE}}')
  AND RunTs <= Timestamp('{{UNTIL}}')
  AND (
    Branch = '{{BRANCH}}'
    OR Branch = 'origin/{{BRANCH}}'
    OR EndsWith(CAST(Branch AS String), '/{{BRANCH}}')
  )
  -- AND (StartsWith(Suite, 'UploadTpch') OR StartsWith(Suite, 'Tpch') OR …)
ORDER BY RunTs, DbAlias, Suite;
