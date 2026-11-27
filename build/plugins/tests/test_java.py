import ntpath
from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest

import java


def make_unit():
    unit = Mock()
    unit.get.return_value = 'project'
    unit.resolve_arc_path.side_effect = lambda path: path
    return unit


@pytest.mark.parametrize('spelling', ['relative', 'root-variable', 'typed-source'])
@pytest.mark.parametrize('relative_path', ['src/resources', 'src/resource files'])
def test_source_directory_check_uses_native_macro(monkeypatch, spelling, relative_path):
    path = {
        'relative': relative_path,
        'root-variable': '${ARCADIA_ROOT}/project/' + relative_path,
        'typed-source': '$S/project/' + relative_path,
    }[spelling]
    unit = make_unit()
    is_dir = Mock()
    monkeypatch.setattr(java.os.path, 'isdir', is_dir)

    # FILES-only declarations use the same check without requiring glob patterns.
    java._CHECK_JAVA_SRCDIR(unit, path)

    unit.oncheck_srcdir.assert_called_once_with('${ARCADIA_ROOT}/project/' + relative_path)
    unit.resolve.assert_not_called()
    unit.set_property.assert_not_called()
    unit.onsrcdir.assert_not_called()
    is_dir.assert_not_called()


@pytest.mark.parametrize('args', [('SKIP_CHECK_SRCDIR', 'resources'), ('resources', 'SKIP_CHECK_SRCDIR')])
def test_skip_source_directory_check(monkeypatch, args):
    unit = Mock()
    is_dir = Mock()
    monkeypatch.setattr(java.os.path, 'isdir', is_dir)

    java._CHECK_JAVA_SRCDIR(unit, *args)

    assert unit.mock_calls == []
    is_dir.assert_not_called()


@pytest.mark.parametrize(
    'path,resolved',
    [
        ('${ARCADIA_BUILD_ROOT}/generated', '${ARCADIA_BUILD_ROOT}/generated'),
        ('$B/generated', '$B/generated'),
        ('$UNRESOLVED', ''),
    ],
)
def test_non_source_directory_is_not_observed(monkeypatch, path, resolved):
    unit = Mock()
    unit.resolve_arc_path.return_value = resolved
    is_dir = Mock()
    monkeypatch.setattr(java.os.path, 'isdir', is_dir)

    java._CHECK_JAVA_SRCDIR(unit, path)

    unit.resolve.assert_not_called()
    unit.oncheck_srcdir.assert_not_called()
    unit.set_property.assert_not_called()
    unit.onsrcdir.assert_not_called()
    is_dir.assert_not_called()


def test_each_source_directory_check_calls_native_macro():
    unit = make_unit()

    java._CHECK_JAVA_SRCDIR(unit, 'first')
    java._CHECK_JAVA_SRCDIR(unit, 'second')

    assert unit.oncheck_srcdir.call_args_list == [
        call('${ARCADIA_ROOT}/project/first'),
        call('${ARCADIA_ROOT}/project/second'),
    ]


def test_relative_source_directory_keeps_first_argument_semantics():
    unit = make_unit()

    java._CHECK_JAVA_SRCDIR(unit, 'first', 'second')

    unit.oncheck_srcdir.assert_called_once_with('${ARCADIA_ROOT}/project/first')
    unit.resolve.assert_not_called()


@pytest.mark.parametrize(
    'path',
    [r'src\resources', '${ARCADIA_ROOT}/project/src/resources'],
)
def test_windows_source_directory_check_uses_portable_path(monkeypatch, path):
    windows_path = Mock(wraps=ntpath)
    monkeypatch.setattr(java, 'os', SimpleNamespace(path=windows_path, sep=ntpath.sep))
    unit = make_unit()

    java._CHECK_JAVA_SRCDIR(unit, path)

    unit.oncheck_srcdir.assert_called_once_with('${ARCADIA_ROOT}/project/src/resources')
    unit.resolve.assert_not_called()
    windows_path.isdir.assert_not_called()
