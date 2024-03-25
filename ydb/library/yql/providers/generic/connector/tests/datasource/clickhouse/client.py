from typing import TypeAlias
import abc
from datetime import datetime
import sys
import time

import clickhouse_connect
from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings

Client: TypeAlias = clickhouse_connect.driver.client.Client


def make_client(s: Settings.ClickHouse) -> Client:
    start = datetime.now()
    attempt = 0

    while (datetime.now() - start).total_seconds() < 60:
        attempt += 1
        try:
            client = clickhouse_connect.get_client(
                host=s.host_external, port=s.http_port_external, username=s.username, password=s.password
            )
        except Exception as e:
            sys.stderr.write(f"attempt #{attempt}: {e}\n")
            time.sleep(5)
            continue

        return client

    raise Exception(f"Failed to connect ClickHouse in {attempt} attempt(s)")

