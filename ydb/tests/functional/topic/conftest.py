# -*- coding: utf-8 -*-
from ydb.tests.oss.canonical import set_canondata_root

pytest_plugins = 'ydb.tests.library.fixtures'


def pytest_configure(config):
    set_canondata_root('ydb/tests/functional/topic/canondata')
