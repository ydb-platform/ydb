-- Per-run per-query series (datetime points — no day averaging).
-- Replace {{SINCE}} e.g. 2026-06-08T00:00:00Z
-- BranchNorm must match generate.py norm_branch() (cloud_* → trunk).
$base = (
    SELECT
        IF(
            COALESCE(Branch, '') != '' AND NOT StartsWith(COALESCE(Branch, ''), '.'),
            Branch,
            IF(
                COALESCE(CiBranch, '') != '' AND NOT StartsWith(COALESCE(CiBranch, ''), '.'),
                CiBranch,
                IF(
                    COALESCE(CAST(Version AS String), '') != ''
                    AND NOT StartsWith(COALESCE(CAST(Version AS String), ''), '.')
                    AND FIND(CAST(Version AS String), '.') IS NOT NULL,
                    SubString(
                        CAST(Version AS String),
                        0U,
                        RFIND(CAST(Version AS String), '.')
                    ),
                    IF(
                        CAST(DbAlias AS String) LIKE '%cloud_%',
                        'trunk',
                        'unknown'
                    )
                )
            )
        ) AS BranchNorm,
        DbAlias,
        Suite,
        Test,
        Run_start_timestamp AS Ts,
        YdbSumMeans,
        CAST(Success AS Int32) AS Success,
        Color,
        CAST(diff_response AS Int32) AS DiffResponse,
        CAST(Suite_not_runned AS Bool) AS SuiteNotRunned,
        Report,
        Version,
        CiVersion,
        CiBranch
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
    DiffResponse,
    Report,
    Version,
    CiVersion,
    CiBranch
FROM $base
ORDER BY Ts, DbAlias, Suite, Test;
