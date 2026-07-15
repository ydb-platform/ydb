import ymake


@ymake.macro
def REGISTER_YQL_PYTHON_UDF(
    unit: ymake.Unit, *, NAME: str = 'CustomPython', RESOURCE_NAME: str = '', ADD_LIBRA_MODULES: str = 'no'
):
    if not RESOURCE_NAME:
        RESOURCE_NAME = NAME
    add_libra_modules = ADD_LIBRA_MODULES == 'yes'

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

    path = NAME + '.yql_python_udf.cpp'
    libra_flag = '1' if add_libra_modules else '0'
    unit.onrun_python3(
        [
            'build/scripts/gen_yql_python_udf.py',
            flavor,
            NAME,
            RESOURCE_NAME,
            path,
            libra_flag,
            'OUT',
            path,
            'OUTPUT_INCLUDES',
        ]
        + output_includes
    )
