select 

    Db ,
    Suite, 
    Test,
    Next_Run_start_timestamp ,
    Run_start_timestamp,
    Run_number_in_version,
    Run_number_in_branch_desc,
    MaxDuration ,
    MeanDuration ,
    MedianDuration ,
    MinDuration ,
    YdbSumMax ,
    YdbSumMeans ,
    YdbSumMin ,
    Version,
    Branch,
    diff_response ,
    Timestamp,
    COALESCE(Success ,0) as Success ,
    max(Kind) OVER (PARTITION  by Db , Run_start_timestamp, Suite) as Kind,
    max(Report) OVER (PARTITION  by Db , Run_start_timestamp, Suite) as Report,
    max(RunId) OVER (PARTITION  by Db , Run_start_timestamp, Suite) as RunId,
    max(RunTs) OVER (PARTITION  by Db , Run_start_timestamp, Suite) as RunTs,
    CASE
        WHEN YdbSumMeans IS NULL THEN true
        ELSE false
    END AS errors,
    CASE
        WHEN max(Report) OVER (PARTITION  by Db , Run_start_timestamp, Suite) IS NULL THEN true
        ELSE false
    END AS Suite_not_runned

 from (

SELECT
    null_template.Db AS Db,  --only from null_template
    COALESCE(real_data.Kind, null_template.Kind) AS Kind,
    COALESCE(real_data.MaxDuration, null_template.MaxDuration) AS MaxDuration,
    COALESCE(real_data.MeanDuration, null_template.MeanDuration) AS MeanDuration,
    COALESCE(real_data.MedianDuration, null_template.MedianDuration) AS MedianDuration,
    COALESCE(real_data.MinDuration, null_template.MinDuration) AS MinDuration,
    null_template.Next_Run_start_timestamp AS Next_Run_start_timestamp,  --only from null_template
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
    COALESCE(real_data.diff_response, null_template.diff_response) AS diff_response

FROM (
    SELECT 
        all_tests.*,
        launch_times.*
    FROM (
        SELECT 
            launch_times.*,
            all_suites.*
        FROM (
            SELECT DISTINCT 
                Db, 
                Version, 
                Branch, 
                Run_start_timestamp, 
                Run_number_in_version, 
                Next_Run_start_timestamp,
                ROW_NUMBER() OVER (PARTITION BY Db, Branch ORDER BY Run_start_timestamp DESC) AS Run_number_in_branch_desc
            FROM (
                SELECT 
                    Db, 
                    Version, 
                    Run_start_timestamp, 
                    Next_Run_start_timestamp,
                    ROW_NUMBER() OVER (PARTITION BY t1.Db, t1.Version ORDER BY t1.Run_start_timestamp ASC) AS Run_number_in_version,
                    Unicode::SplitToList(Version, '.')[0] AS Branch
                FROM (
                    SELECT 
                        runs.Db AS Db, 
                        runs.Version AS Version,
                        run_start.Run_start_timestamp AS Run_start_timestamp,
                        run_start.Next_Run_start_timestamp AS Next_Run_start_timestamp
                    FROM (
                        SELECT DISTINCT
                            Db, 
                            Timestamp,
                            JSON_VALUE(Info, "$.cluster.version") AS Version,
                            CAST(RunId / 1000 AS Timestamp) AS RunTs
                        FROM `perfomance/olap/tests_results`
                        WHere Timestamp >= CurrentUtcDate() - 30*Interval("P1D")
                    ) AS runs
                    LEFT JOIN (
                        SELECT 
                            Db,
                            JSON_VALUE(Info, "$.cluster.version") AS Version,
                            Timestamp AS Run_start_timestamp,
                            LEAD(Timestamp) OVER (PARTITION BY Db, JSON_VALUE(Info, "$.cluster.version") ORDER BY Timestamp) AS Next_Run_start_timestamp
                        FROM `perfomance/olap/tests_results`
                        WHERE Suite = 'Clickbench' AND Test = '_Verification'
                        And Timestamp >= CurrentUtcDate() - 30*Interval("P1D")
                        ORDER BY Db, Run_start_timestamp DESC, Version
                    ) AS run_start
                    ON runs.Db = run_start.Db AND runs.Version = run_start.Version
                    WHERE (
                        (runs.Timestamp >= run_start.Run_start_timestamp AND runs.Timestamp < run_start.Next_Run_start_timestamp) OR 
                        (runs.Timestamp >= run_start.Run_start_timestamp AND run_start.Next_Run_start_timestamp IS NULL)
                    )
                ) AS t1
                GROUP BY Db, Version, Run_start_timestamp, Next_Run_start_timestamp
            ) AS run_start
            GROUP BY Db, Branch, Version, Run_start_timestamp, Run_number_in_version, Next_Run_start_timestamp
        ) AS launch_times
        CROSS JOIN (
            SELECT 
                Suite, Test 
            FROM (
                SELECT 
                    Suite, 
                    ListSort(AGG_LIST_DISTINCT(Test)) AS Tests
                FROM `perfomance/olap/tests_results`
                WHere Timestamp >= CurrentUtcDate() - 30*Interval("P1D")
                GROUP BY Suite 
                ORDER BY Suite
            ) 
            FLATTEN LIST BY Tests AS Test
            ORDER BY Suite, Test
        ) AS all_suites
    ) AS launch_times
    LEFT JOIN (
        SELECT 
            all_tests.*,
            JSON_VALUE(Info, "$.report_url") AS Report,
            JSON_VALUE(all_tests.Info, "$.cluster.version") AS Version_n,
            CAST(JSON_VALUE(Stats, '$.DiffsCount') AS INT) AS diff_response,
            IF(Success > 0, MeanDuration / 1000) AS YdbSumMeans,
            IF(Success > 0, MaxDuration / 1000) AS YdbSumMax,
            IF(Success > 0, MinDuration / 1000) AS YdbSumMin,
            CAST(RunId / 1000 AS Timestamp) AS RunTs
        FROM `perfomance/olap/tests_results` AS all_tests
        Where   JSON_VALUE(all_tests.Info, "$.cluster.version") is Null --and Test != '_Verification'
        and Timestamp >= CurrentUtcDate() - 30*Interval("P1D")
    ) AS all_tests
    ON all_tests.Db = launch_times.Db
    AND all_tests.Suite = launch_times.Suite
    AND all_tests.Test = launch_times.Test
   -- WHERE (  all_tests.Version_n is Null)
    

    ORDER BY Run_start_timestamp DESC, Db, launch_times.Version, RunId
) AS null_template
Full OUTER join 
(SELECT
    real_data.Db AS Db,
    real_data.Kind AS Kind,
    real_data.MaxDuration AS MaxDuration,
    real_data.MeanDuration AS MeanDuration,
    real_data.MedianDuration AS MedianDuration,
    real_data.MinDuration AS MinDuration,
    real_data.Next_Run_start_timestamp AS Next_Run_start_timestamp,
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
    
    FROM (
        SELECT 
            all_tests.*,
            launch_times.*,

        FROM (
            SELECT 
                launch_times.*,
                all_suites.*
            FROM (
                SELECT DISTINCT 
                    Db, 
                    Version, 
                    Branch, 
                    Run_start_timestamp, 
                    Run_number_in_version, 
                    Next_Run_start_timestamp,
                    ROW_NUMBER() OVER (PARTITION BY Db, Branch ORDER BY Run_start_timestamp DESC) AS Run_number_in_branch_desc
                FROM (
                    SELECT 
                        Db, 
                        Version, 
                        Run_start_timestamp, 
                        Next_Run_start_timestamp,
                        ROW_NUMBER() OVER (PARTITION BY t1.Db, t1.Version ORDER BY t1.Run_start_timestamp ASC) AS Run_number_in_version,
                        Unicode::SplitToList(Version, '.')[0] AS Branch
                    FROM (
                        SELECT 
                            runs.Db AS Db, 
                            runs.Version AS Version,
                            run_start.Run_start_timestamp AS Run_start_timestamp,
                            run_start.Next_Run_start_timestamp AS Next_Run_start_timestamp
                        FROM (
                            SELECT DISTINCT
                                Db, 
                                Timestamp,
                                JSON_VALUE(Info, "$.cluster.version") AS Version,
                                CAST(RunId / 1000 AS Timestamp) AS RunTs
                            FROM `perfomance/olap/tests_results`
                            WHere Timestamp >= CurrentUtcDate() - 30*Interval("P1D")
                        ) AS runs
                        LEFT JOIN (
                            SELECT 
                                Db,
                                JSON_VALUE(Info, "$.cluster.version") AS Version,
                                Timestamp AS Run_start_timestamp,
                                LEAD(Timestamp) OVER (PARTITION BY Db, JSON_VALUE(Info, "$.cluster.version") ORDER BY Timestamp) AS Next_Run_start_timestamp
                            FROM `perfomance/olap/tests_results`
                            WHERE Suite = 'Clickbench' AND Test = '_Verification'
                            And Timestamp >= CurrentUtcDate() - 30*Interval("P1D")
                            ORDER BY Db, Run_start_timestamp DESC, Version
                        ) AS run_start
                        ON runs.Db = run_start.Db AND runs.Version = run_start.Version
                        WHERE (
                            (runs.Timestamp >= run_start.Run_start_timestamp AND runs.Timestamp < run_start.Next_Run_start_timestamp) OR 
                            (runs.Timestamp >= run_start.Run_start_timestamp AND run_start.Next_Run_start_timestamp IS NULL)
                        )
                    ) AS t1
                    GROUP BY Db, Version, Run_start_timestamp, Next_Run_start_timestamp
                ) AS run_start
                GROUP BY Db, Branch, Version, Run_start_timestamp, Run_number_in_version, Next_Run_start_timestamp
            ) AS launch_times
            CROSS JOIN (
                SELECT 
                    Suite, Test 
                FROM (
                    SELECT 
                        Suite, 
                        ListSort(AGG_LIST_DISTINCT(Test)) AS Tests
                    FROM `perfomance/olap/tests_results`
                    WHere Timestamp >= CurrentUtcDate() - 30*Interval("P1D")
                    GROUP BY Suite 
                    ORDER BY Suite
                    
                ) 
                FLATTEN LIST BY Tests AS Test
                ORDER BY Suite, Test
            ) AS all_suites
        ) AS launch_times
        LEFT JOIN (
            SELECT 
                all_tests.*,
                JSON_VALUE(Info, "$.report_url") AS Report,
                JSON_VALUE(all_tests.Info, "$.cluster.version") AS Version_n,
                CAST(JSON_VALUE(Stats, '$.DiffsCount') AS INT) AS diff_response,
                IF(Success > 0, MeanDuration / 1000) AS YdbSumMeans,
                IF(Success > 0, MaxDuration / 1000) AS YdbSumMax,
                IF(Success > 0, MinDuration / 1000) AS YdbSumMin,
                CAST(RunId / 1000 AS Timestamp) AS RunTs
            FROM `perfomance/olap/tests_results` AS all_tests
            WHere Timestamp >= CurrentUtcDate() - 30*Interval("P1D")
        ) AS all_tests
        ON all_tests.Db = launch_times.Db
        AND all_tests.Suite = launch_times.Suite
        AND all_tests.Test = launch_times.Test
        AND all_tests.Version_n = launch_times.Version
        WHERE (
            (all_tests.Timestamp >= launch_times.Run_start_timestamp AND all_tests.Timestamp < launch_times.Next_Run_start_timestamp) OR
            (all_tests.Timestamp >= launch_times.Run_start_timestamp AND launch_times.Next_Run_start_timestamp IS NULL) 
            OR all_tests.RunId IS NULL 
        
        )
        
        ORDER BY Run_start_timestamp DESC, Db, Version, RunId
    ) AS real_data
) as real_data

on null_template.Db = real_data.Db
and null_template.Run_start_timestamp = real_data.Run_start_timestamp
and null_template.Suite = real_data.Suite
and null_template.Test = real_data.Test
)



