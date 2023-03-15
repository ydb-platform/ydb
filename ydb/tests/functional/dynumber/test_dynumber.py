# -*- coding: utf-8 -*-
import os
from ydb.tests.oss.ydb_sdk_import import ydb


def test_dynumber():
    config = ydb.DriverConfig(database=os.getenv("YDB_DATABASE"), endpoint=os.getenv("YDB_ENDPOINT"))
    table_name = os.path.join("/", os.getenv("YDB_DATABASE"), "table")
    with ydb.Driver(config) as driver:
        driver.wait(timeout=5)
        session = ydb.retry_operation_sync(lambda: driver.table_client.session().create())
        session.create_table(
            table_name,
            ydb.TableDescription()
            .with_primary_key('key')
            .with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.DyNumber)),
            )
        )

        for value in ["DyNumber(\".149e4\")", "DyNumber(\"15e2\")", "DyNumber(\"150e1\")", "DyNumber(\".151e4\")", "DyNumber(\"1500.1\")"]:
            session.transaction().execute(
                "upsert into `%s` (key ) values (%s );" % (table_name, value),
                commit_tx=True,
            )

        result = session.transaction().execute("select count(*) cnt from `%s`" % table_name, commit_tx=True)
        assert result[0].rows[0].cnt == 4
