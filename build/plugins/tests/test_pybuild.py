import re

import pytest

import coverage
import pybuild

MODULE = 'project/lib'
OTHER = 'project/other'


class FakeUnit:
    def __init__(self, source_root, variables):
        self.source_root = source_root
        self.variables = variables
        self.macro_calls = []

    def __getattr__(self, name):
        if not name.startswith('on'):
            raise AttributeError(name)
        return lambda *args: self.macro_calls.append((name, args))

    def path(self):
        return '$S/' + MODULE

    def get(self, name):
        return self.variables.get(name, '')

    def resolve(self, path):
        assert path == '$S'
        return self.source_root

    def resolve_arc_path(self, path):
        return '$S/{}/{}'.format(MODULE, path)


@pytest.fixture
def source_root(tmp_path, monkeypatch):
    sources = {
        MODULE + '/plain.pyx': '',
        MODULE + '/with_local_include.pyx': 'include "../other/local.pxi"',
        MODULE + '/with_root_include.pyx': 'include "{}/root.pyx"'.format(OTHER),
        OTHER + '/local.pxi': '',
        OTHER + '/root.pyx': '',
    }
    for name, text in sources.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    monkeypatch.setattr(
        pybuild.ymake,
        'parse_cython_includes',
        lambda data: re.findall(rb'include "([^"]+)"', data),
        raising=False,
    )
    # Filters are calculated once per configuration
    coverage.get_coverage_filters.__defaults__[0].clear()
    yield str(tmp_path)
    coverage.get_coverage_filters.__defaults__[0].clear()


@pytest.mark.parametrize(
    'variables, source, expected',
    [
        ({}, 'plain.pyx', False),
        ({'CYTHON_COVERAGE': 'yes'}, 'plain.pyx', True),
        ({'CYTHON_COVERAGE': 'yes', 'FORCE_COVERAGE_DISABLED': 'yes'}, 'plain.pyx', False),
        ({'CYTHON_COVERAGE': 'yes', 'COVERAGE_TARGET_REGEXP': MODULE}, 'plain.pyx', True),
        ({'CYTHON_COVERAGE': 'yes', 'COVERAGE_TARGET_REGEXP': OTHER}, 'plain.pyx', False),
        (
            {'CYTHON_COVERAGE': 'yes', 'COVERAGE_TARGET_REGEXP': OTHER, 'FORCE_COVERAGE_ENABLED': 'yes'},
            'plain.pyx',
            True,
        ),
        ({'CYTHON_COVERAGE': 'yes', 'COVERAGE_EXCLUDE_REGEXP': '.*lib.*'}, 'plain.pyx', False),
        ({'CYTHON_COVERAGE': 'yes', 'COVERAGE_TARGET_REGEXP': OTHER}, 'with_local_include.pyx', True),
        ({'CYTHON_COVERAGE': 'yes', 'COVERAGE_TARGET_REGEXP': OTHER}, 'with_root_include.pyx', True),
        (
            {'CYTHON_COVERAGE': 'yes', 'COVERAGE_TARGET_REGEXP': OTHER, 'COVERAGE_EXCLUDE_REGEXP': '.*other.*'},
            'with_root_include.pyx',
            False,
        ),
    ],
)
def test_cython_coverage_obeys_filters_per_source(source_root, variables, source, expected):
    unit = FakeUnit(source_root, variables)

    assert pybuild.is_cython_coverage_enabled(unit, [(source, 'mod')], py3=True) is expected


def test_cython_coverage_skips_contrib(source_root, monkeypatch):
    unit = FakeUnit(source_root, {'CYTHON_COVERAGE': 'yes'})
    monkeypatch.setattr(unit, 'resolve_arc_path', lambda path: '$S/contrib/python/lib/' + path)

    assert not pybuild.is_cython_coverage_enabled(unit, [('missing.pyx', 'mod')], py3=True)


def test_user_cython_directive_overrides_coverage_linetrace(source_root):
    unit = FakeUnit(source_root, {'CYTHON_COVERAGE': 'yes', '_PYTHON_VER': 'py3'})

    pybuild.PY_SRCS(unit, 'CYTHON_DIRECTIVE', 'linetrace=False', 'plain.pyx')

    (cython_args,) = [args[0] for name, args in unit.macro_calls if name == 'on_buildwith_cython_cpp_dep']
    assert [arg for arg in cython_args if arg.startswith('linetrace')] == ['linetrace=True', 'linetrace=False']


def test_root_relative_includes_are_reported_on_demand(source_root):
    args = (MODULE + '/with_root_include.pyx', 'with_root_include.pyx', source_root)

    assert list(pybuild.parse_pyx_includes(*args)) == []
    assert [name for name, _ in pybuild.parse_pyx_includes(*args, root_relative=True)] == [OTHER + '/root.pyx']


def test_missing_include_is_reported_for_including_file(source_root, tmp_path, monkeypatch):
    (tmp_path / MODULE / 'broken.pyx').write_text('include "missing.pxi"')
    errors = []
    monkeypatch.setattr(pybuild.ymake, 'report_configure_error', errors.append, raising=False)

    assert list(pybuild.parse_pyx_includes(MODULE + '/broken.pyx', 'broken.pyx', source_root)) == []
    assert len(errors) == 1
    assert errors[0].startswith("'broken.pyx' includes missing file: missing.pxi")


def test_resource_cython_coverage_follows_root_relative_include(source_root):
    unit = FakeUnit(
        source_root,
        {'CYTHON_COVERAGE': 'yes', '_USE_RESOURCE_CYTHON': 'yes', 'COVERAGE_TARGET_REGEXP': OTHER},
    )

    assert pybuild.is_cython_coverage_enabled(unit, [('with_root_include.pyx', 'mod')], py3=True)
