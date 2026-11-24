from unittest.mock import Mock

import pytest

from yql_prebuilt import _YQL_INTERNALIZED_SOURCE_FINALIZE, _YQL_PREBUILT_FINALIZE


@pytest.mark.parametrize('sources,peers', [('wrapper.cpp', 'example/impl'), (None, None)])
def test_internalized_source_dependencies(sources, peers):
    unit = Mock()
    unit.get.side_effect = {'_YQL_SOURCE_SRCS': sources, '_YQL_SOURCE_PEERS': peers}.get
    _YQL_INTERNALIZED_SOURCE_FINALIZE(unit)
    unit.onsrcs.assert_called_once_with((sources or '').split())
    unit.onpeerdir.assert_called_once_with((peers or '').split())
    unit.on_add_yql_udf_deps_contrib.assert_not_called()


@pytest.mark.parametrize(
    'kind,filename,command',
    [('PROGRAM', 'sample', '_YQL_PROGRAM_LINK_CMD'), ('UDF', 'libsample.so', 'YQL_UDF_LINK_CMD')],
)
@pytest.mark.parametrize('resource', ['123', None, '', 'None'])
def test_cached_file_or_sources(kind, filename, command, resource):
    unit = Mock()
    unit.get.side_effect = {
        '_YQL_PREBUILT_KIND': kind,
        '_YQL_PREBUILT_RESOURCE_ID': resource,
        '_YQL_SOURCE_SRCS': 'main.cpp',
        '_YQL_SOURCE_PEERS': 'library/cpp/getopt',
        '_YQL_SOURCE_SUPPRESSIONS': 'lsan.supp',
    }.get
    unit.filename.return_value = filename

    _YQL_PREBUILT_FINALIZE(unit)

    if resource == '123':
        unit.onfrom_sandbox.assert_called_once_with(
            ['FILE', resource, 'RENAME', 'RESOURCE', 'OUT_NOAUTO', filename + '.prebuilt', 'EXECUTABLE']
        )
        unit.onprimary_output.assert_called_once_with(['${BINDIR}/' + filename + '.prebuilt'])
        unit.set.assert_called_once_with([command, '$_YQL_PREBUILT_TOOL_CMD'])
        unit.onsrcs.assert_not_called()
        unit.onpeerdir.assert_not_called()
        unit.onsuppressions.assert_not_called()
        unit.on_add_yql_udf_deps_contrib.assert_not_called()
    else:
        unit.onfrom_sandbox.assert_not_called()
        unit.onprimary_output.assert_not_called()
        unit.set.assert_not_called()
        unit.onsrcs.assert_called_once_with(['main.cpp'])
        unit.onpeerdir.assert_called_once_with(['library/cpp/getopt'])
        unit.onsuppressions.assert_called_once_with(['lsan.supp'])
        if kind == 'UDF':
            unit.on_add_yql_udf_deps_contrib.assert_called_once_with([])
        else:
            unit.on_add_yql_udf_deps_contrib.assert_not_called()
