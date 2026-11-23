from unittest.mock import Mock, call, patch

import pytest

from yql_prebuilt import _YQL_PREBUILT_FINALIZE
from yql_python_udf import REGISTER_YQL_PYTHON_UDF, _YQL_PYTHON_UDF_SOURCE_FINALIZE


def make_unit(values):
    unit = Mock()
    unit.get.side_effect = values.get
    unit.set.side_effect = lambda args: values.update({args[0]: args[1]})
    unit.filename.return_value = 'libpython_udf.so'
    return unit


@pytest.mark.parametrize('version,name', [('2', 'ArcPython2'), ('3', 'Python3')])
@pytest.mark.parametrize('resource', ['123', '0', None, '', 'None'])
@pytest.mark.parametrize('libra', ['yes', 'no'])
def test_prebuilt_python_registration(version, name, resource, libra):
    values = {
        '_YQL_PREBUILT_KIND': 'PYTHON_UDF',
        '_YQL_PREBUILT_PYTHON_VERSION': version,
        '_YQL_PREBUILT_RESOURCE_ID': resource,
        'USE_ARCADIA_PYTHON': 'yes',
        'PYTHON3': 'yes' if version == '3' else 'no',
    }
    unit = make_unit(values)
    unit.on_yql_python_udf_source_finalize.side_effect = lambda _: _YQL_PYTHON_UDF_SOURCE_FINALIZE(unit)

    REGISTER_YQL_PYTHON_UDF(unit, NAME=name, RESOURCE_NAME='Python' + version, ADD_LIBRA_MODULES=libra)
    unit.onpeerdir.assert_not_called()
    unit.onrun_python3.assert_not_called()
    _YQL_PREBUILT_FINALIZE(unit)

    if resource in ('123', '0'):
        unit.onfrom_sandbox.assert_called_once()
        assert unit.onfrom_sandbox.call_args.args[0][1] == resource
        unit.on_yql_python_udf_source_finalize.assert_not_called()
        unit.onpeerdir.assert_not_called()
        unit.onrun_python3.assert_not_called()
    else:
        unit.onfrom_sandbox.assert_not_called()
        setup = unit.on_make_yql_python3_udf if version == '3' else unit.on_make_yql_python_udf
        setup.assert_called_once_with([])
        unit.onrun_python3.assert_called_once()
        arguments = unit.onrun_python3.call_args.args[0]
        assert arguments[:6] == [
            'build/scripts/gen_yql_python_udf.py',
            'Arcadia',
            name,
            'Python' + version,
            name + '.yql_python_udf.cpp',
            '1' if libra == 'yes' else '0',
        ]
        libra_peer = call(['quality/user_sessions/libra_arc/noyql'])
        assert (libra_peer in unit.onpeerdir.call_args_list) == (libra == 'yes')


@pytest.mark.parametrize('name', ['Python2', 'Python3', 'ArcPython2', 'SystemPython3_13', 'CustomPython'])
@pytest.mark.parametrize('resource_name', ['', 'resource_alias'])
@pytest.mark.parametrize('arcadia_python', ['yes', 'no'])
def test_source_registration_is_immediate(name, resource_name, arcadia_python):
    unit = make_unit({'USE_ARCADIA_PYTHON': arcadia_python})
    REGISTER_YQL_PYTHON_UDF(unit, NAME=name, RESOURCE_NAME=resource_name)
    unit.set.assert_not_called()
    arguments = unit.onrun_python3.call_args.args[0]
    assert arguments[1:4] == ['Arcadia' if arcadia_python == 'yes' else 'System', name, resource_name or name]


def test_prebuilt_default_registration():
    values = {'_YQL_PREBUILT_KIND': 'PYTHON_UDF', '_YQL_PREBUILT_PYTHON_VERSION': '3'}
    unit = make_unit(values)
    REGISTER_YQL_PYTHON_UDF(unit)
    _YQL_PYTHON_UDF_SOURCE_FINALIZE(unit)
    assert unit.onrun_python3.call_args.args[0][2:4] == ['CustomPython', 'CustomPython']


def test_source_without_registration():
    unit = make_unit({'_YQL_PREBUILT_PYTHON_VERSION': '3'})
    _YQL_PYTHON_UDF_SOURCE_FINALIZE(unit)
    unit.on_make_yql_python3_udf.assert_called_once_with([])
    unit.onrun_python3.assert_not_called()


def test_multiple_prebuilt_registrations():
    unit = make_unit({'_YQL_PREBUILT_KIND': 'PYTHON_UDF', '_YQL_PREBUILT_PYTHON_VERSION': '3'})
    REGISTER_YQL_PYTHON_UDF(unit, NAME='Python3')
    REGISTER_YQL_PYTHON_UDF(unit, NAME='CustomPython', RESOURCE_NAME='CustomResource')
    _YQL_PYTHON_UDF_SOURCE_FINALIZE(unit)
    assert [args.args[0][2:4] for args in unit.onrun_python3.call_args_list] == [
        ['Python3', 'Python3'],
        ['CustomPython', 'CustomResource'],
    ]


@pytest.mark.parametrize('kind', [None, 'PYTHON_UDF'])
def test_invalid_name_is_reported_before_cache_selection(kind):
    unit = make_unit({'_YQL_PREBUILT_KIND': kind})
    with patch('yql_python_udf.ymake.report_configure_error', create=True) as error:
        REGISTER_YQL_PYTHON_UDF(unit, NAME='invalid')
    error.assert_called_once_with('Invalid python UDF module name')


def test_opensource_libra_registration_error():
    unit = make_unit({'OPENSOURCE': 'yes'})
    with patch('yql_python_udf.ymake.report_configure_error', create=True) as error:
        REGISTER_YQL_PYTHON_UDF(unit, ADD_LIBRA_MODULES='yes')
    error.assert_called_once_with('Libra modules are not supported in opensource python UDFs')
    assert unit.onrun_python3.call_args.args[0][5] == '0'
    assert call(['quality/user_sessions/libra_arc/noyql']) not in unit.onpeerdir.call_args_list
