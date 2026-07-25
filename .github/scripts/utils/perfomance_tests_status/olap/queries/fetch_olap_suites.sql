-- OLAP suite runs for the report window (from suites data mart).
-- Replace {{SINCE}} with ISO timestamp, e.g. 2026-05-24T00:00:00Z
-- Default report window: ~2 months.
-- Prefer compact columns; for 2m windows fetch in ~3-day chunks (MCP/clients may truncate).
-- Optional: separate query for FailTests where FailCount > 0.
SELECT
  Branch,
  Version,
  CiBranch,
  CiVersion,
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
  AND (
    StartsWith(Suite, 'Clickbench')
    OR StartsWith(Suite, 'Tpch')
    OR StartsWith(Suite, 'Tpcds')
    OR StartsWith(Suite, 'UploadTpch')
    OR StartsWith(Suite, 'WorkloadManager')
  )
ORDER BY RunTs, DbAlias, Suite;
