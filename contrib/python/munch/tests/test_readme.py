import doctest
import os
import pytest

import yatest.common as yc
_HERE = os.path.abspath(os.path.dirname(yc.source_path(__file__)))
_README_PATH = os.path.join(_HERE, '..', 'README.md')
assert os.path.exists(_README_PATH)


@pytest.mark.usefixtures("yaml")
def test_readme():
    globs = {
        'print_function': print
    }
    result = doctest.testfile(_README_PATH, module_relative=False, globs=globs)
    assert not result.failed
