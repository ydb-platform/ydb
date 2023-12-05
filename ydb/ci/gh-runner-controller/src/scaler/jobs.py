from .ch import Clickhouse


def get_jobs_summary(ch: Clickhouse):
    # adapted from https://clickhouse.com/blog/monitor-github-action-workflow-job-queues

    RUNNER_TYPE_LABELS = ["auto-provisioned"]

    # noinspection SqlShouldBeInGroupBy,SqlResolve,SqlNoDataSourceInspection
    QUEUE_QUERY = f"""SELECT
        last_status AS status,
        toUInt32(count()) AS length,
        labels
    FROM
    (
        SELECT
            arraySort(groupArray(status))[-1] AS last_status,
            labels,
            id,
            html_url
        FROM workflow_jobs
        WHERE has(labels, 'self-hosted')
            AND hasAny(arrayMap(x -> lower(x), {RUNNER_TYPE_LABELS}), arrayMap(x -> lower(x), labels))
            AND NOT has(labels, 'ARM64')
            AND started_at > now() - INTERVAL 6 HOUR
        GROUP BY ALL
        HAVING last_status IN ('in_progress', 'queued')
    )
    GROUP BY ALL
    ORDER BY labels, last_status"""
    result = ch.select(QUEUE_QUERY)

    queued = in_progress = 0
    # TODO: add multiple labels support
    for r in result:
        status = r["status"]
        if status == "queued":
            queued += r["length"]
        elif status == "in_progress":
            in_progress += r["length"]
        else:
            raise Exception(f"Unknown status {status}")

    return queued, in_progress
