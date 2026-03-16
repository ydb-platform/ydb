import os
import pytest


# https://a.yandex-team.ru/arc/trunk/arcadia/library/testing/recipes/clickhouse/__main__.py?rev=5637204#L104
def get_clickhouse_url():
    try:
        return "http://localhost:{}".format(os.environ["RECIPE_CLICKHOUSE_HTTP_PORT"])
    except KeyError:
        pytest.fail("ClickHouse is either not running, or the port is unknown. See source code for details")
