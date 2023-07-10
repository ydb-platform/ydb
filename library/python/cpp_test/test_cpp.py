import json
import os
import subprocess

import pytest
import yaml
import yatest

from library.python.testing.style import rules
import library.python.resource as lpr


STYLE_CONFIG_JSON_12 = json.dumps(yaml.safe_load(lpr.find('/cpp_style/config/12')))
STYLE_CONFIG_JSON_14 = json.dumps(yaml.safe_load(lpr.find('/cpp_style/config/14')))

RES_FILE_PREFIX = '/cpp_style/files/'
CHECKED_PATHS = list(lpr.iterkeys(RES_FILE_PREFIX, strip_prefix=True))


def check_style(filename, actual_source):
    try:
        clang_format_binary = yatest.common.binary_path('contrib/libs/clang12/tools/clang-format/clang-format')
        config = STYLE_CONFIG_JSON_12
    except Exception:
        clang_format_binary = yatest.common.binary_path('contrib/libs/clang14/tools/clang-format/clang-format')
        config = STYLE_CONFIG_JSON_14

    command = [clang_format_binary, '-assume-filename=' + filename, '-style=' + config]
    styled_source = subprocess.check_output(command, input=actual_source)

    assert actual_source.decode() == styled_source.decode()


@pytest.mark.parametrize('path', CHECKED_PATHS)
def test_cpp_style(path):
    data = lpr.find(RES_FILE_PREFIX + path)
    skip_reason = rules.get_skip_reason(path, data, skip_links=False)
    if skip_reason:
        raise pytest.skip("style check is omitted: {}".format(skip_reason))
    else:
        check_style(os.path.basename(path), data)
