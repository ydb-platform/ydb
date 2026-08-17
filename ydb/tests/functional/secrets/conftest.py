# -*- coding: utf-8 -*-
# Must live in conftest.py and be listed in TEST_SRCS: chunked py3test only ships
# those files, so pytest_plugins in test modules is not applied before fixture setup.
pytest_plugins = ["ydb.tests.functional.secrets.lib.secrets_plugin"]
