import dataclasses
import logging
from .ch import Clickhouse

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class JobQueue:
    preset: str
    in_queue: int
    in_progress: int


def get_jobs_summary(ch: Clickhouse):
    # adapted from https://clickhouse.com/blog/monitor-github-action-workflow-job-queues

    RUNNER_TYPE_LABELS = ["auto-provisioned"]

    # noinspection SqlShouldBeInGroupBy,SqlResolve,SqlNoDataSourceInspection,SqlAggregates,SqlUnused
    QUEUE_QUERY = f"""SELECT
        labels,
        toUInt32(countIf(last_status='queued')) AS count_queued,
        toUInt32(countIf(last_status='in_progress')) AS count_in_progress,
        groupArray(run_id) as run_ids
    FROM
    (
        SELECT
            arraySort(groupArray(status))[-1] AS last_status,
            labels,
            id,
            run_id,
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
    ORDER BY labels"""

    ch_result = ch.select(QUEUE_QUERY)

    logger.info("ch_result=%r", ch_result)

    result = []
    for r in ch_result:
        preset_name = "default"
        for k in r['labels']:
            if k.startswith('build-preset'):
                preset_name = k.replace('build-preset-', '')

        result.append(
            #
            JobQueue(
                preset=preset_name,
                in_queue=r["count_queued"],
                in_progress=r["count_in_progress"]
            )
        )

    return result
