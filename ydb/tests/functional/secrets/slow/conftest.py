# -*- coding: utf-8 -*-
# Separate PY3TEST target; parent secrets/conftest.py is not in this suite's bundle.
pytest_plugins = ["ydb.tests.functional.secrets.lib.secrets_plugin"]
