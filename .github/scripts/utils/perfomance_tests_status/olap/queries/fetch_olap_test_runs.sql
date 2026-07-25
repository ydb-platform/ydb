-- Per-run per-query series (datetime points — no day averaging).
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
        Run_start_timestamp AS Ts,
        YdbSumMeans,
        CAST(Success AS Int32) AS Success,
        Color,
        CAST(Suite_not_runned AS Bool) AS SuiteNotRunned,
        Report
    FROM `perfomance/olap/fast_results`
    WHERE Run_start_timestamp >= Timestamp('{{SINCE}}')
      AND Test NOT IN ('_Verification', 'Sum')
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
    Ts,
    YdbSumMeans AS ydb,
    Success,
    Color,
    Report
FROM $base
ORDER BY Ts, DbAlias, Suite, Test;
