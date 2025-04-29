# -*- coding: utf-8 -*-
import os
import yatest


def kikimr_driver_path():
    if os.getenv("YDB_DRIVER_BINARY"):
        return yatest.common.binary_path(os.getenv("YDB_DRIVER_BINARY"))

    return yatest.common.binary_path("kikimr/driver/kikimr")


def ydb_cli_path():
    if os.getenv("YDB_CLI_BINARY"):
        return yatest.common.binary_path(os.getenv("YDB_CLI_BINARY"))

    return yatest.common.binary_path("ydb/apps/ydb/ydb")
