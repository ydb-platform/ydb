# -*- coding: utf-8 -*-

import time

from ydb.tests.functional.secrets.lib.secrets_plugin import DATABASE, run_with_assert

CLUSTER_CONFIG = dict(
    additional_log_configs={
        # 'TX_PROXY': LogLevels.DEBUG,
    },
)


def _cell_to_str(value):
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _secret_value_not_leaked_to_sysviews(config, secret_value):
    sys_table = f"`{DATABASE}/.sys/top_queries_by_cpu_time_one_minute`"
    query = f"""
    SELECT QueryText FROM {sys_table} WHERE String::Contains(QueryText, "ALTER SECRET")
    """
    result_sets = run_with_assert(config, query)
    if not result_sets or not result_sets[0].rows:
        return False
    for row in result_sets[0].rows:
        text = _cell_to_str(row["QueryText"])
        if secret_value in text:
            return True
    return False


def _secret_value_not_leaked_to_query_sessions(config, secret_value):
    sys_table = f"`{DATABASE}/.sys/query_sessions`"
    query = f"""
    SELECT Query FROM {sys_table} WHERE String::Contains(Query, "ALTER SECRET")
    """
    result_sets = run_with_assert(config, query)
    if not result_sets or not result_sets[0].rows:
        return False
    for row in result_sets[0].rows:
        text = _cell_to_str(row["Query"])
        if text and secret_value in text:
            return True
    return False


def test_secret_value_not_leaked_to_sysviews(db_fixture):
    secret_value = "secret-rand-value-svcpu9monq"
    secret_name = "sec"
    run_with_assert(
        db_fixture,
        f"CREATE SECRET `{secret_name}` WITH (value='{secret_value}');",
    )
    for _ in range(10):
        run_with_assert(
            db_fixture,
            f"ALTER SECRET `{secret_name}` WITH (value='{secret_value}');",
        )

    # We'll check in 'one_minute' sysview, so we need to wait for two minutes.
    time.sleep(120)

    assert not _secret_value_not_leaked_to_sysviews(db_fixture, secret_value), (
        "secret literal must not appear in ALTER SECRET rows in " ".sys/top_queries_by_cpu_time_one_minute"
    )

    assert not _secret_value_not_leaked_to_query_sessions(
        db_fixture, secret_value
    ), "secret literal must not appear in .sys/query_sessions column Query"
