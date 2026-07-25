# -*- coding: utf-8 -*-
from contextlib import contextmanager

import requests

DATABASE = '/Root'


def mon_base_url(cluster, node_index=1):
    node = cluster.nodes[node_index]
    return f'https://{node.host}:{node.mon_port}'


def run_viewer_query(base_url, query, database=DATABASE, token='root@builtin', timeout=5):
    response = requests.post(
        base_url + '/viewer/query',
        headers={'Authorization': token},
        params={'database': database, 'query': query, 'schema': 'multi'},
        verify=False,
        timeout=timeout,
    )
    assert response.status_code == 200, response.text
    return response


@contextmanager
def grants_provided(base_url, object_path, *permissions, database=DATABASE):
    grantees = '`database@builtin`, `viewer@builtin`, `monitoring@builtin`'
    perms = ', '.join(f"'{permission}'" for permission in permissions)
    run_viewer_query(
        base_url,
        f"GRANT {perms} ON `{object_path}` TO {grantees};",
        database=database,
    )
    try:
        yield
    finally:
        run_viewer_query(
            base_url,
            f"REVOKE {perms} ON `{object_path}` FROM {grantees};",
            database=database,
        )


def grant_describe_schema_provided(base_url, database=DATABASE):
    return grants_provided(base_url, database, 'ydb.granular.describe_schema', database=database)


@contextmanager
def with_topic(base_url, topic_name, database=DATABASE):
    run_viewer_query(base_url, f'CREATE TOPIC `{topic_name}`;', database=database)
    try:
        yield f'{database}/{topic_name}'
    finally:
        run_viewer_query(base_url, f'DROP TOPIC `{topic_name}`;', database=database)
