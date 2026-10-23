-- Daily per-query series for charts / date-window recompute.
-- Replace {{SINCE}} e.g. 2026-06-08T00:00:00Z
$base = (
    SELECT
        IF(
            COALESCE(Branch, '') != '' AND NOT StartsWith(COALESCE(Branch, ''), '.'),
            Branch,
            IF(
                COALESCE(CiBranch, '') != '' AND NOT StartsWith(COALESCE(CiBranch, ''), '.'),
                CiBranch,
                'unknown'
            )
        ) AS BranchNorm,
        DbAlias,
        Suite,
        Test,
        CAST(DateTime::MakeDate(Run_start_timestamp) AS String) AS Day,
        YdbSumMeans,
        CAST(Success AS Int32) AS Success,
        Color,
        CAST(errors AS Bool) AS IsError,
        CAST(Suite_not_runned AS Bool) AS SuiteNotRunned,
        Report
    FROM `perfomance/olap/fast_results`
    WHERE Run_start_timestamp >= Timestamp('{{SINCE}}')
      AND Test NOT IN ('_Verification', 'Sum')
      -- skip mart placeholders (suite expected but not executed) — not real query fails
      AND COALESCE(CAST(Suite_not_runned AS Bool), false) = false
      AND (
          StartsWith(Suite, 'Clickbench')
          OR StartsWith(Suite, 'Tpch')
          OR StartsWith(Suite, 'Tpcds')
          OR StartsWith(Suite, 'UploadTpch')
          OR StartsWith(Suite, 'WorkloadManager')
      )
      AND DbAlias IN (
          'sas_big_column',
          'sas_small_column',
          'cloud_slonnn_64_column',
          'cloud_slonnn_128_column',
          'vla_big_column',
          'vla_small_column',
          'vla_3_node_column'
      )
);

SELECT
    BranchNorm AS Branch,
    DbAlias,
    Suite,
    Test,
    Day,
    COUNT(*) AS n,
    -- real fails only: executed query with Success=0 (Color set); skip null-templates
    COUNTIF(Success = 0 AND Color IS NOT NULL) AS fails,
    AVG_IF(YdbSumMeans, YdbSumMeans IS NOT NULL) AS ydb,
    MAX(Report) AS Report
FROM $base
GROUP BY BranchNorm, DbAlias, Suite, Test, Day
ORDER BY Day, DbAlias, Suite, Test;
