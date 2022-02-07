# -*- coding: utf-8 -*-
from ydb import *  # noqa
import sys
import six
import warnings

warnings.warn("module kikimr.public.sdk.python.client is deprecated. please use ydb instead")


for name, module in six.iteritems(sys.modules.copy()):
    if not name.startswith("ydb"):
        continue

    if name.startswith("ydb.public"):
        continue

    module_import_path = name.split('.')
    if len(module_import_path) < 2:
        continue

    sys.modules['kikimr.public.sdk.python.client.' + '.'.join(module_import_path[1:])] = module
