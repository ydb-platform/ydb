$test_data = (
    SELECT 
        a.*,
        suite_folder || '/' || test_name AS full_name,
        MIN(run_timestamp) OVER (PARTITION BY commit) as postcommit_start_run_timestamp,
        MIN(job_id) OVER (PARTITION BY commit) as postcommit_start_job_id,

        a.commit || '_' || a.pull AS run_identifier
    FROM `test_results/test_runs_column` AS a
    WHERE 
        
        a.run_timestamp >= CurrentUtcDate() - 1 * Interval("P1D")
        AND a.branch = 'main'
        AND a.build_type = 'relwithdebinfo'
        AND a.job_name LIKE 'Postcommit%'
);

-- Count test runs for each test by commit
$test_runs_by_commit_test = (
    SELECT 
        postcommit_start_run_timestamp,
        commit,
        postcommit_start_job_id,
        full_name,
        COUNT(DISTINCT run_identifier) AS run_count,
        CASE WHEN COUNT(DISTINCT run_identifier) > 1 THEN 1 ELSE 0 END AS has_retries,
        COUNT(DISTINCT run_identifier) - 1 AS retry_count
    FROM $test_data
    GROUP BY postcommit_start_run_timestamp,postcommit_start_job_id, commit, full_name
);

-- Агрегация по коммитам для подсчета максимального количества ретраев
$max_retries_by_commit = (
    SELECT
        commit,
        postcommit_start_run_timestamp,
        postcommit_start_job_id,
        COUNT(DISTINCT full_name) AS total_tests,
        MAX(retry_count) AS max_retries,
    FROM $test_runs_by_commit_test
    GROUP BY commit,postcommit_start_job_id, postcommit_start_run_timestamp
);



-- Итоговый результат
SELECT
    m.commit as commit,
    m.postcommit_start_run_timestamp as postcommit_start_run_timestamp,
    m.total_tests as total_tests,
    m.postcommit_start_job_id as postcommit_start_job_id,
   
    m.max_retries  as max_retries,
FROM $max_retries_by_commit m

