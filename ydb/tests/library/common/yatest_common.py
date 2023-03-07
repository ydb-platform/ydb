# -*- coding: utf-8 -*-
import os

import yatest.common as ya_common
import yatest.common.network as ya_common_network
import ydb.tests.oss.canonical as oss_canonical
"""
For yatest.common package see file
library/python/testing/yatest_common/yatest/common/__init__.py
"""

PortManager = ya_common_network.PortManager


def wrap(func, alternative):
    def wrapped(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
        except (NotImplementedError, AttributeError):
            result = alternative(*args, **kwargs)
        return result

    return wrapped


def canonical_file(*args, **kwargs):
    if oss_canonical.is_oss:
        assert kwargs['local']
        path = kwargs.get('path', args[0] if len(args) else None)
        path = os.path.abspath(path)
        with open(path, 'r') as test_file, open(oss_canonical.canondata_filepath(path), 'r') as canon_file:
            assert canon_file.read() == test_file.read()
    else:
        return ya_common.canonical_file(*args, **kwargs)


source_path = wrap(ya_common.source_path, lambda x: x)
build_path = wrap(ya_common.build_path, lambda x: x)
binary_path = wrap(ya_common.binary_path, lambda x: x)
output_path = wrap(ya_common.output_path, lambda x: x)
work_path = wrap(ya_common.work_path, lambda x: x)

get_param = wrap(ya_common.get_param, lambda x, y=None: y)
get_param_dict_copy = wrap(ya_common.get_param_dict_copy, lambda: dict())


def get_bool_param(key, default):
    val = get_param(key, default)
    if isinstance(val, bool):
        return val

    return val.lower() == 'true'


class Context(object):

    @property
    def test_name(self):
        return wrap(lambda: ya_common.context.test_name, lambda: None)()

    @property
    def sanitize(self):
        return wrap(lambda: ya_common.context.sanitize, lambda: None)()


context = Context()

ExecutionError = ya_common.ExecutionError

execute = ya_common.execute


def plain_or_under_sanitizer(plain, sanitized):
    """
    Meant to be used in test code for constants (timeouts, etc)
    See also arcadia/util/system/sanitizers.h

    :return: plain if no sanitizer enabled or sanitized otherwise
    """
    return plain if not context.sanitize else sanitized
