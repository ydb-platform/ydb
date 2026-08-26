-- Per-query (Test) regressions vs baseline for OLAP suites.
-- Source: perfomance/olap/fast_results
-- Replace:
--   {{SINCE}}        e.g. 2026-06-08T00:00:00Z
--   {{BASE_END}}     e.g. 2026-06-22T00:00:00Z   (since + 14d)
--   {{RECENT_FROM}}  e.g. 2026-07-10T00:00:00Z   (last ~14d)
-- Tip: if the client truncates, run once per DbAlias (filter IN-list).

$raw = (
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
        CAST(errors AS Bool) AS IsError
    FROM `perfomance/olap/fast_results`
    WHERE Run_start_timestamp >= Timestamp('{{SINCE}}')
      AND Test NOT IN ('_Verification', 'Sum')
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

$early = (
    SELECT
        BranchNorm,
        DbAlias,
        Suite,
        Test,
        COUNT(*) AS n_early,
        COUNTIF(Success = 0 OR IsError) AS fails_early,
        AVG_IF(YdbSumMeans, YdbSumMeans IS NOT NULL) AS ydb_early
    FROM $raw
    WHERE Ts < Timestamp('{{BASE_END}}')
    GROUP BY BranchNorm, DbAlias, Suite, Test
);

$late = (
    SELECT
        BranchNorm,
        DbAlias,
        Suite,
        Test,
        COUNT(*) AS n_late,
        COUNTIF(Success = 0 OR IsError) AS fails_late,
        AVG_IF(YdbSumMeans, YdbSumMeans IS NOT NULL) AS ydb_late
    FROM $raw
    WHERE Ts >= Timestamp('{{RECENT_FROM}}')
    GROUP BY BranchNorm, DbAlias, Suite, Test
);

SELECT
    COALESCE(e.BranchNorm, l.BranchNorm) AS Branch,
    COALESCE(e.DbAlias, l.DbAlias) AS DbAlias,
    COALESCE(e.Suite, l.Suite) AS Suite,
    COALESCE(e.Test, l.Test) AS Test,
    e.n_early AS n_early,
    l.n_late AS n_late,
    e.fails_early AS fails_early,
    l.fails_late AS fails_late,
    CAST(e.fails_early AS Double) / MAX_OF(e.n_early, 1) AS fail_rate_early,
    CAST(l.fails_late AS Double) / MAX_OF(l.n_late, 1) AS fail_rate_late,
    e.ydb_early AS ydb_early,
    l.ydb_late AS ydb_late,
    IF(
        e.ydb_early IS NOT NULL AND e.ydb_early > 0 AND l.ydb_late IS NOT NULL,
        (l.ydb_late - e.ydb_early) / e.ydb_early * 100.0,
        NULL
    ) AS ydb_pct
FROM $early AS e
FULL JOIN $late AS l
    ON e.BranchNorm = l.BranchNorm
   AND e.DbAlias = l.DbAlias
   AND e.Suite = l.Suite
   AND e.Test = l.Test
WHERE
    -- keep only interesting query-level signals
    (
        CAST(l.fails_late AS Double) / MAX_OF(l.n_late, 1) >= 0.03
        AND CAST(l.fails_late AS Double) / MAX_OF(l.n_late, 1)
            > CAST(e.fails_early AS Double) / MAX_OF(e.n_early, 1) + 0.02
    )
    OR (
        e.ydb_early IS NOT NULL AND e.ydb_early > 0 AND l.ydb_late IS NOT NULL
        AND (l.ydb_late - e.ydb_early) / e.ydb_early >= 0.10
    )
    OR (
        CAST(l.fails_late AS Double) / MAX_OF(l.n_late, 1) >= 0.50
    )
ORDER BY
    CAST(l.fails_late AS Double) / MAX_OF(l.n_late, 1) DESC,
    ydb_pct DESC;
