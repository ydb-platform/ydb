SELECT
    sampled_at,
    `date` AS event_date,
    kind,
    name,
    COALESCE(workflow, '') AS workflow,
    COALESCE(job_name, '') AS job_name,
    COALESCE(branch, '') AS branch,
    COALESCE(run_url, '') AS run_url,
    COALESCE(pr_number, '') AS pr_number,
    COALESCE(pr_title, '') AS pr_title,
    COALESCE(state, '') AS state,
    COALESCE(zone, '') AS zone,
    cores,
    ram_gib,
    nrd_gib,
    limit_value,
    usage_value,
    free_value,
    created_at,
    age_sec
FROM `analytics/ci_capacity`
WHERE `date` >= CurrentUtcDate() - Interval("P90D")
  AND (
    kind != 'runner'
    OR `date` >= CurrentUtcDate() - Interval("P7D")
  )
