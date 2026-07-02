#!/usr/bin/env python
# -*- coding: utf-8 -*-
import importlib.util

import yatest.common

_SOURCE_PATH = yatest.common.source_path('ydb/tests/functional/sqs/common/test_multiplexing_tables_format.py')
_SPEC = importlib.util.spec_from_file_location('_sqs_migration_source', _SOURCE_PATH)
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
for _export_name in dir(_MODULE):
    if not _export_name.startswith('_'):
        globals()[_export_name] = getattr(_MODULE, _export_name)
del _SOURCE_PATH, _SPEC, _MODULE, _export_name
