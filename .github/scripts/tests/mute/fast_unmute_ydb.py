"""YDB access for ``fast_unmute_active`` and ``fast_unmute_grace``."""

import datetime
import logging
import os

import ydb

from mute.constants import get_manual_unmute_currently_muted_lookback_days

_SIMULATE_UNMUTED_ENV = 'MANUAL_UNMUTE_SIMULATE_UNMUTED'


def _simulate_unmuted_full_names():
    raw = os.environ.get(_SIMULATE_UNMUTED_ENV, '').strip()
    if not raw:
        return frozenset()
    return frozenset(x.strip() for x in raw.split(',') if x.strip())


def _escape(value):
    return str(value).replace("'", "''")


def _coerce_dt(value):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.timezone.utc)
        return value.astimezone(datetime.timezone.utc)
    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time.min, tzinfo=datetime.timezone.utc)
    if isinstance(value, int):
        return datetime.datetime.fromtimestamp(value / 1_000_000, tz=datetime.timezone.utc)
    if isinstance(value, float):
        return datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc)
    return None


def create_manual_unmute_table(ydb_wrapper, table_path):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_path}` (
        `full_name`            Utf8      NOT NULL,
        `branch`               Utf8      NOT NULL,
        `build_type`           Utf8      NOT NULL,
        `github_issue_number`  Uint64    NOT NULL,
        `requested_at`         Timestamp NOT NULL,
        `window_days`          Uint32    NOT NULL,
        PRIMARY KEY (full_name, branch, build_type)
    )
    WITH (STORE = COLUMN)
    """
    ydb_wrapper.create_table(table_path, create_sql)


def fetch_all_rows(ydb_wrapper, table_path):
    query = f"""
    SELECT full_name, branch, build_type, github_issue_number, requested_at, window_days
    FROM `{table_path}`
    """
    return ydb_wrapper.execute_scan_query(query, query_name='manual_unmute_fetch_all')


def count_rows_per_issue(ydb_wrapper, table_path, issue_numbers):
    """Return {issue_number: remaining_row_count} for the given issues."""
    numbers = sorted({int(n) for n in (issue_numbers or []) if n is not None})
    if not numbers:
        return {}
    in_list = ','.join(str(n) for n in numbers)
    query = f"""
    SELECT github_issue_number AS n, COUNT(*) AS c
    FROM `{table_path}`
    WHERE github_issue_number IN ({in_list})
    GROUP BY github_issue_number
    """
    rows = ydb_wrapper.execute_scan_query(query, query_name='manual_unmute_count_remaining')
    return {int(r['n']): int(r['c']) for r in rows if r.get('n') is not None}


def fetch_candidate_issues(ydb_wrapper, issues_table_path, lookback_days):
    query = f"""
    SELECT issue_id, issue_number, body
    FROM `{issues_table_path}`
    WHERE state = 'CLOSED'
        AND state_reason = 'COMPLETED'
        AND closed_at >= CurrentUtcTimestamp() - {int(lookback_days)} * Interval("P1D")
    """
    return ydb_wrapper.execute_scan_query(query, query_name='manual_unmute_candidate_issues')


def fetch_currently_muted(ydb_wrapper, tests_monitor_path, branch, build_type):
    lb = int(get_manual_unmute_currently_muted_lookback_days())
    br = _escape(branch)
    bt = _escape(build_type)
    query = f"""
    SELECT t.full_name AS full_name
    FROM `{tests_monitor_path}` AS t
    INNER JOIN (
        SELECT full_name AS fn, MAX(date_window) AS max_date_window
        FROM `{tests_monitor_path}`
        WHERE branch = '{br}'
            AND build_type = '{bt}'
            AND date_window >= CurrentUtcDate() - {lb} * Interval("P1D")
        GROUP BY full_name
    ) AS last_row
        ON t.full_name = last_row.fn AND t.date_window = last_row.max_date_window
    WHERE t.branch = '{br}'
        AND t.build_type = '{bt}'
        AND t.is_muted = 1
    """
    rows = ydb_wrapper.execute_scan_query(query, query_name='manual_unmute_currently_muted')
    result = {row['full_name'] for row in rows if row.get('full_name')}
    pretend_unmuted = _simulate_unmuted_full_names()
    if pretend_unmuted:
        logging.warning(
            '%s active — excluding from currently-muted (simulate is_muted=0): %s',
            _SIMULATE_UNMUTED_ENV,
            ', '.join(sorted(pretend_unmuted)),
        )
        result -= pretend_unmuted
    return result


def create_fast_unmute_grace_table(ydb_wrapper, table_path):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_path}` (
        `full_name`                   Utf8      NOT NULL,
        `branch`                      Utf8      NOT NULL,
        `build_type`                  Utf8      NOT NULL,
        `github_issue_number`         Uint64    NOT NULL,
        `fast_track_requested_at`     Timestamp NOT NULL,
        `grace_started_at`            Timestamp NOT NULL,
        `grace_ttl_days`              Uint32    NOT NULL,
        PRIMARY KEY (full_name, branch, build_type)
    )
    WITH (STORE = COLUMN)
    """
    ydb_wrapper.create_table(table_path, create_sql)


def upsert_fast_unmute_grace_row(
    ydb_wrapper,
    table_path,
    full_name,
    branch,
    build_type,
    github_issue_number,
    fast_track_requested_at,
    grace_started_at,
    grace_ttl_days,
):
    column_types = (
        ydb.BulkUpsertColumns()
        .add_column('full_name', ydb.PrimitiveType.Utf8)
        .add_column('branch', ydb.PrimitiveType.Utf8)
        .add_column('build_type', ydb.PrimitiveType.Utf8)
        .add_column('github_issue_number', ydb.PrimitiveType.Uint64)
        .add_column('fast_track_requested_at', ydb.PrimitiveType.Timestamp)
        .add_column('grace_started_at', ydb.PrimitiveType.Timestamp)
        .add_column('grace_ttl_days', ydb.PrimitiveType.Uint32)
    )
    rows = [
        {
            'full_name': full_name,
            'branch': branch,
            'build_type': build_type,
            'github_issue_number': int(github_issue_number),
            'fast_track_requested_at': fast_track_requested_at,
            'grace_started_at': grace_started_at,
            'grace_ttl_days': int(grace_ttl_days),
        }
    ]
    ydb_wrapper.bulk_upsert(table_path, rows, column_types)


def expire_fast_unmute_grace(ydb_wrapper, table_path):
    """Remove grace rows after ``grace_ttl_days`` calendar days since ``grace_started_at``."""
    query = f"""
    SELECT full_name, branch, build_type, grace_started_at, grace_ttl_days
    FROM `{table_path}`
    """
    try:
        rows = ydb_wrapper.execute_scan_query(query, query_name='fast_unmute_grace_expire_scan')
    except Exception as exc:
        logging.warning('expire_fast_unmute_grace: scan failed: %s', exc)
        return

    today = datetime.datetime.now(tz=datetime.timezone.utc).date()

    for row in rows:
        gs = _coerce_dt(row.get('grace_started_at'))
        if not gs:
            continue
        gs_date = gs.astimezone(datetime.timezone.utc).date()
        threshold = max(1, int(row['grace_ttl_days']))
        if (today - gs_date).days >= threshold:
            delete_grace_row(
                ydb_wrapper,
                table_path,
                row.get('full_name'),
                row.get('branch'),
                row.get('build_type'),
            )


def delete_grace_row(ydb_wrapper, table_path, full_name, branch, build_type):
    if not full_name or not branch or not build_type:
        return
    query = f"""
    DECLARE $full_name AS Utf8;
    DECLARE $branch AS Utf8;
    DECLARE $build_type AS Utf8;
    DELETE FROM `{table_path}`
    WHERE full_name = $full_name
        AND branch = $branch
        AND build_type = $build_type;
    """
    ydb_wrapper.execute_dml(
        query,
        {'$full_name': full_name, '$branch': branch, '$build_type': build_type},
        query_name='fast_unmute_grace_delete',
    )


def upsert_rows(ydb_wrapper, table_path, rows):
    if not rows:
        return
    column_types = (
        ydb.BulkUpsertColumns()
        .add_column('full_name', ydb.PrimitiveType.Utf8)
        .add_column('branch', ydb.PrimitiveType.Utf8)
        .add_column('build_type', ydb.PrimitiveType.Utf8)
        .add_column('github_issue_number', ydb.PrimitiveType.Uint64)
        .add_column('requested_at', ydb.PrimitiveType.Timestamp)
        .add_column('window_days', ydb.PrimitiveType.Uint32)
    )
    ydb_wrapper.bulk_upsert(table_path, rows, column_types)


def delete_row(ydb_wrapper, table_path, full_name, branch, build_type):
    query = f"""
    DECLARE $full_name AS Utf8;
    DECLARE $branch AS Utf8;
    DECLARE $build_type AS Utf8;
    DELETE FROM `{table_path}`
    WHERE full_name = $full_name
        AND branch = $branch
        AND build_type = $build_type;
    """
    ydb_wrapper.execute_dml(
        query,
        {'$full_name': full_name, '$branch': branch, '$build_type': build_type},
        query_name='manual_unmute_delete_row',
    )
