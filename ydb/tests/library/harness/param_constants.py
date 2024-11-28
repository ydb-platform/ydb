# -*- coding: utf-8 -*-
import os
import yatest


def kikimr_driver_path():
    if os.getenv("YDB_DRIVER_BINARY"):
        return yatest.common.binary_path(os.getenv("YDB_DRIVER_BINARY"))

    return yatest.common.binary_path("kikimr/driver/kikimr")


def kikimr_configure_binary_path():
    return yatest.common.binary_path("ydb/tools/cfg/bin/ydb_configure")
