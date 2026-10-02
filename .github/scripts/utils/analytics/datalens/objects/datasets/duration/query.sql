SELECT
    t.date AS event_date,
    CAST(t.run_id AS Utf8) AS ci_run_id,
    CAST(t.run_attempt AS Utf8) AS run_attempt,
    CAST(t.github_job_id AS Int64) AS github_job_id,
    CAST(t.pr_number AS Int64) AS pr_number,
    t.workflow AS workflow,
    t.job_name AS job_name,
    t.build_preset AS build_preset,
    t.branch AS branch,
    t.event_name AS event_name,
    t.commit AS commit,
    t.name AS step_name,
    t.source AS source,
    t.event_ts AS start_ts,
    t.value AS duration_ms,
    t.value / 1000.0 AS duration_sec,
    t.conclusion AS conclusion,
    j.job_conclusion AS job_conclusion,
    t.run_url AS run_url,
    JSON_VALUE(t.labels, "$.ya_attempt") AS ya_try
FROM `analytics/ci_metrics` AS t

LEFT JOIN (
    SELECT
        github_job_id,
        MAX(conclusion) AS job_conclusion
    FROM `analytics/ci_metrics`
    WHERE kind = 'duration'
      AND source = 'github_job'
      AND name = 'job'
      AND date >= CurrentUtcDate() - Interval("P90D")
    GROUP BY github_job_id
) AS j ON j.github_job_id = t.github_job_id

WHERE t.kind = 'duration'
    AND t.value > 0
    AND (
        t.conclusion IS NULL
        OR t.conclusion != 'skipped'
        OR (t.source = 'github_job' AND t.name = 'job')
    )
    AND (
        (t.source = 'github_job' AND t.name IN ('job', 'queue'))
        OR (t.source = 'github_step' AND t.name IN ('Checkout', 'Build and test'))
        OR t.source = 'ya_phase'
    )
    AND t.date >= CurrentUtcDate() - Interval("P90D")
