SELECT
    t.date AS event_date,
    CAST(t.run_id AS Utf8) AS run_id,
    CAST(t.github_job_id AS Int64) AS github_job_id,
    CAST(t.run_attempt AS Int64) AS run_attempt,
    CAST(t.pr_number AS Int64) AS pr_number,
    t.workflow AS workflow,
    t.job_name AS job_name,
    t.build_preset AS build_preset,
    t.source AS source,
    t.name AS step_name,
    t.event_ts AS start_ts,
    t.value AS duration_ms,
    t.conclusion AS conclusion,
    j.job_conclusion AS job_conclusion,
    
    t.run_url AS run_url,
    t.branch AS branch,
    t.commit AS commit,
    Unicode::Substring(CAST(s.started_at AS Utf8), 0, 10) || ' ' || Unicode::Substring(CAST(s.started_at AS Utf8), 11, 5) || ' ' || COALESCE(rs.run_icon, '🟡') || ' · ' || COALESCE(Unicode::Substring(t.commit, 0, 7), '') || ' · ' || CAST(t.run_id AS Utf8) || ' · #' || CAST(COALESCE(t.run_attempt, 1UL) AS Utf8) AS run_label
FROM `analytics/ci_metrics` AS t
INNER JOIN (
    SELECT run_id, run_attempt, MIN(event_ts) AS started_at
    FROM `analytics/ci_metrics`
    WHERE kind = 'duration'
      AND date >= CurrentUtcDate() - Interval("P90D")
    GROUP BY run_id, run_attempt
) AS s ON s.run_id = t.run_id AND s.run_attempt = t.run_attempt

LEFT JOIN (
    SELECT
        run_id,
        run_attempt,
        CASE
            WHEN job_fail = 1 OR (job_seen = 0 AND any_fail = 1) THEN '🔴'
            WHEN job_cancel = 1 OR (job_seen = 0 AND any_cancel = 1) THEN '⚫'
            WHEN job_seen = 1 AND job_success = 1 THEN '🟢'
            WHEN job_seen = 0 AND any_success = 1 AND any_fail = 0 AND any_cancel = 0 THEN '🟢'
            ELSE '🟡'
        END AS run_icon
    FROM (
        SELECT
            run_id,
            run_attempt,
            MAX(CASE WHEN source = 'github_job' AND name = 'job' THEN 1 ELSE 0 END) AS job_seen,
            MAX(CASE WHEN source = 'github_job' AND name = 'job' AND conclusion = 'failure' THEN 1 ELSE 0 END) AS job_fail,
            MAX(CASE WHEN source = 'github_job' AND name = 'job' AND conclusion = 'cancelled' THEN 1 ELSE 0 END) AS job_cancel,
            MAX(CASE WHEN source = 'github_job' AND name = 'job' AND conclusion = 'success' THEN 1 ELSE 0 END) AS job_success,
            MAX(CASE WHEN conclusion = 'failure' THEN 1 ELSE 0 END) AS any_fail,
            MAX(CASE WHEN conclusion = 'cancelled' THEN 1 ELSE 0 END) AS any_cancel,
            MAX(CASE WHEN conclusion = 'success' THEN 1 ELSE 0 END) AS any_success
        FROM `analytics/ci_metrics`
        WHERE kind = 'duration'
          AND date >= CurrentUtcDate() - Interval("P90D")
        GROUP BY run_id, run_attempt
    )
) AS rs ON rs.run_id = t.run_id AND rs.run_attempt = t.run_attempt

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
    AND t.source IN ('github_job', 'github_step', 'ya_phase')
    AND t.name NOT LIKE 'Post %'
    AND t.date >= CurrentUtcDate() - Interval("P90D")
