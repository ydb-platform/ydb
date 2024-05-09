from typing import TypeAlias
from datetime import datetime
import sys
import time

import clickhouse_connect
from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings

Client: TypeAlias = clickhouse_connect.driver.client.Client


def make_client(settings: Settings.ClickHouse) -> Client:
    start = datetime.now()
    attempt = 0

    while (datetime.now() - start).total_seconds() < 60:
        attempt += 1
        try:
            client = clickhouse_connect.get_client(
                host=settings.host_external,
                port=settings.http_port_external,
                username=settings.username,
                password=settings.password,
            )
        except Exception as e:
            sys.stderr.write(f"attempt #{attempt}: {e}\n")
            time.sleep(5)
            continue

        return client

    raise Exception(f"Failed to connect ClickHouse in {attempt} attempt(s)")
