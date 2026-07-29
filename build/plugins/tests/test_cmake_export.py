import pytest

import cmake_export


class FakeUnit:
    def __init__(self, path):
        self._path = path
        self.exported_names = []

    def path(self):
        return self._path

    def oncmake_exported_target_name(self, args):
        self.exported_names.append(args)


@pytest.fixture
def errors(monkeypatch):
    reported = []
    monkeypatch.setattr(cmake_export.ymake, 'report_configure_error', reported.append, raising=False)
    return reported


def test_name_from_path(errors):
    unit = FakeUnit('$S/proj/foo/bar/baz')
    cmake_export.CMAKE_EXPORTED_TARGET_NAME_FROM_PATH(unit, 'proj')
    assert unit.exported_names == [['foo-bar-baz']]
    assert errors == []


def test_root_slashes_stripped(errors):
    unit = FakeUnit('$S/proj/foo/qux')
    cmake_export.CMAKE_EXPORTED_TARGET_NAME_FROM_PATH(unit, '/proj/foo/')
    assert unit.exported_names == [['qux']]
    assert errors == []


def test_module_outside_root(errors):
    unit = FakeUnit('$S/other/foo')
    cmake_export.CMAKE_EXPORTED_TARGET_NAME_FROM_PATH(unit, 'proj')
    assert unit.exported_names == []
    assert len(errors) == 1


def test_module_equals_root(errors):
    unit = FakeUnit('$S/proj/foo')
    cmake_export.CMAKE_EXPORTED_TARGET_NAME_FROM_PATH(unit, 'proj/foo')
    assert unit.exported_names == []
    assert len(errors) == 1


def test_root_matching_partial_fragment(errors):
    unit = FakeUnit('$S/projx/foo')
    cmake_export.CMAKE_EXPORTED_TARGET_NAME_FROM_PATH(unit, 'proj')
    assert unit.exported_names == []
    assert len(errors) == 1


def test_empty_root(errors):
    unit = FakeUnit('$S/proj/foo/bar')
    cmake_export.CMAKE_EXPORTED_TARGET_NAME_FROM_PATH(unit, '/')
    assert unit.exported_names == []
    assert len(errors) == 1


def test_wrong_arity(errors):
    unit = FakeUnit('$S/proj/foo/bar')
    cmake_export.CMAKE_EXPORTED_TARGET_NAME_FROM_PATH(unit)
    cmake_export.CMAKE_EXPORTED_TARGET_NAME_FROM_PATH(unit, 'proj', 'extra')
    assert unit.exported_names == []
    assert len(errors) == 2
