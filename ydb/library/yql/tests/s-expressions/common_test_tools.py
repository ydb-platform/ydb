import os
import shutil
import platform
import codecs
from glob import glob
from collections import OrderedDict

import yatest.common
import pytest

import yql_utils
from yql_utils import get_supported_providers, yql_binary_path, yql_source_path, get_param, new_table, volatile_attrs

DATA_PATH = yql_source_path('ydb/library/yql/tests/s-expressions/suites')
try:
    ASTDIFF_PATH = yql_binary_path('ydb/library/yql/tools/astdiff/astdiff')
except Exception as e:
    ASTDIFF_PATH = None


def pytest_generate_tests_for_part(metafunc, currentPart, partsCount):
    argvalues = []

    suites = [name for name in os.listdir(DATA_PATH) if os.path.isdir(os.path.join(DATA_PATH, name))]

    for suite in suites:
        for case in sorted([os.path.basename(yql_program_path)[:-4]
                            for yql_program_path in glob(os.path.join(DATA_PATH, suite) + '/*.yql')]):
            if hash((suite, case)) % partsCount == currentPart:
                argvalues.append((suite, case))

    metafunc.parametrize(['suite', 'case'], argvalues)


def pytest_generate_tests(metafunc): return pytest_generate_tests_for_part(metafunc, 0, 1)


def get_program_cfg(suite, case):
    return yql_utils.get_program_cfg(suite, case, DATA_PATH)


def cut_unstable(s):
    replacements = OrderedDict((
        (yatest.common.source_path('contrib').replace('\\', '/'), '<source_path>'),
        (yatest.common.source_path('contrib').replace('\\', '\\\\'), '<source_path>'),
        (yatest.common.source_path('').replace('\\', '/'), '<source_path>'),
        (yatest.common.source_path('').replace('\\', '\\\\'), '<source_path>'),
        (
            os.path.normpath(
                os.path.join(
                    yatest.common.binary_path('ydb/library/yql/tools/yqlrun/yqlrun'),
                    '..', '..', '..', '..'
                )
            ),
            '<binary_path>'
        ),
        (yatest.common.build_path(), '<build_path>'),
        (os.path.realpath(yatest.common.build_path()), '<build_path>'),
        ('\r\n', '\n'),
        ('\\\\', '/')
    ))

    for r in replacements:
        s = s.replace(r, replacements[r])
    return s


def one_of(line, attrs):
    for attr in attrs:
        if attr in line:
            return True
    return False


def canonize_yson(yson_text, out_dir):
    mr_yson = '\n'.join(line for line in yson_text.splitlines() if not one_of(line, volatile_attrs))
    mr_yson_file = os.path.join(out_dir, 'mr_results.yson')
    with open(mr_yson_file, 'w') as f:
        f.write(mr_yson)
    return yatest.common.canonical_file(mr_yson_file)


def get_program(suite, case):
    program_file = os.path.join(DATA_PATH, suite, '%s.yql' % case)

    with codecs.open(program_file, encoding='utf-8') as program_file_descr:
        program_text = program_file_descr.read()

    return program_text
