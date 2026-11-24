import pytest

import ytest


class Unit:
    def path(self):
        return '$S/project/module'

    def get(self, name):
        return ''

    def message(self, message):
        pytest.fail(str(message))


@pytest.mark.parametrize('script', ['py3test.bin', 'exectest', 'gofmt'])
def test_dump_test_records_module_directory(script):
    dart = {
        'SCRIPT-REL-PATH': script,
        'SOURCE-FOLDER-PATH': 'project/module/src',
        'TEST-NAME': script,
    }
    data = ytest.dump_test(Unit(), dart)
    assert 'BUILD-FOLDER-PATH: project/module\n' in data
    assert 'SOURCE-FOLDER-PATH: project/module/src\n' in data
    assert 'BUILD-FOLDER-PATH' not in dart


def test_dump_test_preserves_explicit_build_folder_path():
    dart = {
        'SCRIPT-REL-PATH': 'py3test.bin',
        'SOURCE-FOLDER-PATH': 'project/module',
        'BUILD-FOLDER-PATH': 'project/explicit',
        'TEST-NAME': 'test',
    }
    data = ytest.dump_test(Unit(), dart)
    assert 'BUILD-FOLDER-PATH: project/explicit\n' in data
    assert data.count('BUILD-FOLDER-PATH:') == 1
