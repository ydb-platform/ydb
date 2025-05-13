$start_timestamp = (CurrentUtcDate() - 30 * Interval("P1D"));

$all_suites = (
    SELECT 
        Suite, Test 
    FROM (
        SELECT 
            Suite, 
            ListSort(AGG_LIST_DISTINCT(Test)) AS Tests
        FROM `perfomance/olap/tests_results`
        WHERE Timestamp >= $start_timestamp
        GROUP BY Suite 
    ) 
    FLATTEN LIST BY Tests AS Test
);

$launch_times = (
    SELECT 
        launch_times_raw.*,
        all_suites.*
    FROM (
        SELECT
            Db,
            Branch,
            Version,
            LunchId,
            CAST(Min(RunId / 1000UL) AS Timestamp) AS Run_start_timestamp,
            ROW_NUMBER() OVER (PARTITION BY Db, Version ORDER BY Min(RunId) ASC) AS Run_number_in_version,
            ROW_NUMBER() OVER (PARTITION BY Db, Branch ORDER BY Min(RunId) DESC) AS Run_number_in_branch_desc
        FROM `perfomance/olap/tests_results`
        WHERE Timestamp >= $start_timestamp
        GROUP BY
            Db,
            Unicode::SplitToList(JSON_VALUE(Info, "$.cluster.version"), '.')[0] As Branch,
            JSON_VALUE(Info, "$.cluster.version") AS Version,
            JSON_VALUE(Info, "$.ci_launch_id") AS LunchId
    ) AS launch_times_raw
    CROSS JOIN $all_suites AS all_suites
);

$all_tests_raw =
    SELECT 
        tests_results.*,
        JSON_VALUE(Info, "$.report_url") AS Report,
        JSON_VALUE(tests_results.Info, "$.cluster.version") AS Version_n,
        JSON_VALUE(tests_results.Info, "$.ci_launch_id") AS LunchId_n,
        CAST(JSON_VALUE(Stats, '$.DiffsCount') AS INT) AS diff_response,
        IF(Success > 0, MeanDuration / 1000) AS YdbSumMeans,
        IF(Success > 0, MaxDuration / 1000) AS YdbSumMax,
        IF(Success > 0, MinDuration / 1000) AS YdbSumMin,
        CAST(RunId / 1000UL AS Timestamp) AS RunTs,
        IF (JSON_VALUE(Stats, "$.errors.other") = "true",
            "red",
            IF (JSON_VALUE(Stats, "$.errors.timeout") = "true",
            "blue",
                IF (JSON_VALUE(Stats, "$.errors.warning") = "true",
                    "yellow",
                    "green"
                )
            )
        ) AS Color
    FROM `perfomance/olap/tests_results` AS tests_results
    WHERE Timestamp >= $start_timestamp;

SELECT 
    Db,
    Suite, 
    Test,
    Run_start_timestamp,
    Run_number_in_version,
    Run_number_in_branch_desc,
    MaxDuration,
    MeanDuration,
    MedianDuration,
    MinDuration,
    YdbSumMax,
    YdbSumMeans,
    YdbSumMin,
    Version,
    Branch,
    diff_response,
    Timestamp,
    COALESCE(Success,0) AS Success,
    max(Kind) OVER (PARTITION  by Db, Run_start_timestamp, Suite) AS Kind,
    max(Report) OVER (PARTITION  by Db, Run_start_timestamp, Suite) AS Report,
    max(RunId) OVER (PARTITION  by Db, Run_start_timestamp, Suite) AS RunId,
    max(RunTs) OVER (PARTITION  by Db, Run_start_timestamp, Suite) AS RunTs,
    YdbSumMeans IS NULL AS errors,
    max(Report) OVER (PARTITION  by Db, Run_start_timestamp, Suite) IS NULL AS Suite_not_runned,
    Color
FROM (
    SELECT
        null_template.Db AS Db,  --only from null_template
        COALESCE(real_data.Kind, null_template.Kind) AS Kind,
        COALESCE(real_data.MaxDuration, null_template.MaxDuration) AS MaxDuration,
        COALESCE(real_data.MeanDuration, null_template.MeanDuration) AS MeanDuration,
        COALESCE(real_data.MedianDuration, null_template.MedianDuration) AS MedianDuration,
        COALESCE(real_data.MinDuration, null_template.MinDuration) AS MinDuration,
        COALESCE(real_data.Report, null_template.Report) AS Report,
        COALESCE(real_data.Branch, null_template.Branch) AS Branch,
        COALESCE(real_data.RunId, null_template.RunId) AS RunId,
        COALESCE(real_data.RunTs, null_template.RunTs) AS RunTs,
        null_template.Run_number_in_version AS Run_number_in_version,  --only from null_template
        null_template.Run_start_timestamp AS Run_start_timestamp,  --only from null_template
        COALESCE(real_data.Run_number_in_branch_desc, null_template.Run_number_in_branch_desc) AS Run_number_in_branch_desc,
        COALESCE(real_data.Success, null_template.Success) AS Success,
        null_template.Suite AS Suite,  --only from null_template
        null_template.Test AS Test,  --only from null_template
        COALESCE(real_data.Timestamp, null_template.Timestamp) AS Timestamp,
        COALESCE(real_data.Version, null_template.Version) AS Version,
        COALESCE(real_data.YdbSumMax, null_template.YdbSumMax) AS YdbSumMax,
        COALESCE(real_data.YdbSumMeans, null_template.YdbSumMeans) AS YdbSumMeans,
        COALESCE(real_data.YdbSumMin, null_template.YdbSumMin) AS YdbSumMin,
        COALESCE(real_data.diff_response, null_template.diff_response) AS diff_response,
        COALESCE(real_data.Color, null_template.Color) AS Color,
    FROM (
        SELECT 
            all_tests.*,
            launch_times.*
        FROM $launch_times AS launch_times
        LEFT JOIN (
            SELECT
                *
            FROM $all_tests_raw AS all_tests_raw
            WHERE JSON_VALUE(all_tests_raw.Info, "$.cluster.version") is NULL
        ) AS all_tests
        ON all_tests.Db = launch_times.Db
        AND all_tests.Suite = launch_times.Suite
        AND all_tests.Test = launch_times.Test
    -- WHERE (  all_tests.Version_n is NULL)
    ) AS null_template
    FULL OUTER JOIN (
        SELECT
            real_data.Db AS Db,
            real_data.Kind AS Kind,
            real_data.MaxDuration AS MaxDuration,
            real_data.MeanDuration AS MeanDuration,
            real_data.MedianDuration AS MedianDuration,
            real_data.MinDuration AS MinDuration,
            real_data.Report AS Report,
            real_data.Branch AS Branch,
            real_data.RunId AS RunId,
            real_data.RunTs AS RunTs,
            real_data.Run_number_in_version AS Run_number_in_version,
            real_data.Run_start_timestamp AS Run_start_timestamp,
            real_data.Run_number_in_branch_desc AS Run_number_in_branch_desc,
            --real_data.Stats AS Stats,
            real_data.Success AS Success,
            real_data.Suite AS Suite,
            real_data.Test AS Test,
            real_data.Timestamp AS Timestamp,
            real_data.Version AS Version,
            real_data.YdbSumMax AS YdbSumMax,
            real_data.YdbSumMeans AS YdbSumMeans,
            real_data.YdbSumMin AS YdbSumMin,
            real_data.diff_response AS diff_response,
            real_data.Color AS Color,
        FROM (
            SELECT 
                all_tests.*,
                launch_times.*,
            FROM $launch_times AS launch_times
            LEFT JOIN $all_tests_raw AS all_tests
            ON all_tests.Db = launch_times.Db
            AND all_tests.Suite = launch_times.Suite
            AND all_tests.Test = launch_times.Test
            AND all_tests.Version_n = launch_times.Version
            WHERE (
                all_tests.LunchId_n == launch_times.LunchId
                OR all_tests.RunId IS NULL 
            )
        ) AS real_data
    ) AS real_data
    ON null_template.Db = real_data.Db
    AND null_template.Run_start_timestamp = real_data.Run_start_timestamp
    AND null_template.Suite = real_data.Suite
    AND null_template.Test = real_data.Test
)
