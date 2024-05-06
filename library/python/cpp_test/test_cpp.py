import json
import os
import subprocess

import pytest
import yaml
import yatest

from library.python.testing.style import rules
import library.python.resource as lpr


# keep in sync with the logic in https://a.yandex-team.ru/arcadia/devtools/ya/handlers/style/cpp_style.py?rev=r12543375#L21
STYLE_CONFIG_JSON = json.dumps(yaml.safe_load(lpr.find('resfs/file/config.clang-format')))

RES_FILE_PREFIX = '/cpp_style/files/'
CHECKED_PATHS = list(lpr.iterkeys(RES_FILE_PREFIX, strip_prefix=True))


def check_style(filename, actual_source):
    clang_format_binary = yatest.common.binary_path('contrib/libs/clang16/tools/clang-format/clang-format')
    config = STYLE_CONFIG_JSON

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
