import json

import ymake


@ymake.macro
def REGISTER_YQL_PYTHON_UDF(
    unit: ymake.Unit, *, NAME: str = 'CustomPython', RESOURCE_NAME: str = '', ADD_LIBRA_MODULES: str = 'no'
):
    if (
        NAME not in ('Python3', 'Python2', 'ArcPython2')
        and not NAME.startswith('SystemPython')
        and 'CustomPython' not in NAME
    ):
        ymake.report_configure_error('Invalid python UDF module name')

    if not RESOURCE_NAME:
        RESOURCE_NAME = NAME
    if unit.get('_YQL_PREBUILT_KIND') == 'PYTHON_UDF':
        registrations = json.loads(unit.get('_YQL_PYTHON_REGISTRATIONS') or '[]')
        registrations.append([NAME, RESOURCE_NAME, ADD_LIBRA_MODULES])
        unit.set(['_YQL_PYTHON_REGISTRATIONS', json.dumps(registrations)])
    else:
        register_python_udf(unit, NAME, RESOURCE_NAME, ADD_LIBRA_MODULES)


def register_python_udf(unit, name, resource_name, add_libra_modules):
    add_libra_modules = add_libra_modules == 'yes'

    use_arcadia_python = unit.get('USE_ARCADIA_PYTHON') == 'yes'
    py3 = unit.get('PYTHON3') == 'yes'

    if unit.get('OPENSOURCE'):
        if add_libra_modules:
            ymake.report_configure_error('Libra modules are not supported in opensource python UDFs')
            add_libra_modules = False

    yql_python_dir = unit.get('YQL_PYTHON_DIR')
    if not yql_python_dir:
        yql_python_dir = 'yql/essentials/udfs/common/python'

    unit.onyql_abi_version(['2', '45', '0'])
    unit.onpeerdir(['yql/essentials/udfs/common/python/python_udf'])
    unit.onpeerdir(['yql/essentials/public/udf'])

    if add_libra_modules:
        unit.onpeerdir(['quality/user_sessions/libra_arc/noyql'])
        unit.onpeerdir(['yql/udfs/quality/libra/module'])

    if use_arcadia_python:
        flavor = 'Arcadia'
        unit.onpeerdir(
            ['library/python/runtime', '/'.join([yql_python_dir, '/main'])]
            if not py3
            else ['library/python/runtime_py3', 'yql/essentials/udfs/common/python/main_py3']
        )
    else:
        flavor = 'System'

    output_includes = [
        'yql/essentials/udfs/common/python/python_udf/python_udf.h',
        'yql/essentials/public/udf/udf_registrator.h',
    ]
    if add_libra_modules:
        output_includes.append('yql/udfs/quality/libra/module/module.h')

    path = name + '.yql_python_udf.cpp'
    libra_flag = '1' if add_libra_modules else '0'
    unit.onrun_python3(
        [
            'build/scripts/gen_yql_python_udf.py',
            flavor,
            name,
            resource_name,
            path,
            libra_flag,
            'OUT',
            path,
            'OUTPUT_INCLUDES',
        ]
        + output_includes
    )


@ymake.macro
def _YQL_PYTHON_UDF_SOURCE_FINALIZE(unit: ymake.Unit):
    if unit.get('_YQL_PREBUILT_PYTHON_VERSION') == '3':
        unit.on_make_yql_python3_udf([])
    else:
        unit.on_make_yql_python_udf([])
    for registration in json.loads(unit.get('_YQL_PYTHON_REGISTRATIONS') or '[]'):
        register_python_udf(unit, *registration)
