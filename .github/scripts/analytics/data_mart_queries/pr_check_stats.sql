SELECT
    date,
    COUNT(DISTINCT job_id) as jobs_count,
    CAST(SUMIF(duration, attempt == 1) AS Int64) as duration_1,
    CAST(SUMIF(duration, attempt > 1) AS Int64) as duration_rerun,
    100 * SUMIF(duration, attempt > 1) / SUM(duration) as rerun_duration_perc,
    COUNTIF(attempt == 1) as count_1,
    COUNTIF(attempt > 1) as count_rerun,
    100 * CAST(COUNTIF(attempt > 1) AS Double) / count(*) as rerun_count_perc,
FROM (
    SELECT
        job_id,
        duration,
        CAST(COALESCE(String::SplitToList(pull,'_attempt_')[1],'1') as Int32) AS attempt,
        DateTime::MakeDate(run_timestamp) as date
    FROM `test_results/test_runs_column`
    WHERE (job_name = 'PR-check') AND (branch = 'main') AND (build_type = 'relwithdebinfo')
    AND (run_timestamp > CurrentUtcDate() - Interval("P7D"))
)
GROUP BY date
