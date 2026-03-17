#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import snowflake.sqlalchemy
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect

from .util import ischema_names_baseline


def test_type_synonyms():
    from snowflake.sqlalchemy.snowdialect import ischema_names

    for k, _ in ischema_names.items():
        assert getattr(snowflake.sqlalchemy, k) is not None


def test_type_baseline():
    assert set(SnowflakeDialect.ischema_names.keys()) == set(
        ischema_names_baseline.keys()
    )
    for k, v in SnowflakeDialect.ischema_names.items():
        assert issubclass(v, ischema_names_baseline[k])
