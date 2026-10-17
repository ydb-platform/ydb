import ymake


@ymake.macro
def _YQL_PREBUILT_FINALIZE(unit: ymake.Unit):
    program = unit.get('_YQL_PREBUILT_KIND') == 'PROGRAM'
    resource = unit.get('_YQL_PREBUILT_RESOURCE_ID')
    if resource and resource != 'None':
        output = unit.filename() + '.prebuilt'
        unit.onfrom_sandbox(['FILE', resource, 'RENAME', 'RESOURCE', 'OUT_NOAUTO', output, 'EXECUTABLE'])
        unit.onprimary_output(['${BINDIR}/' + output])
        unit.set(['_YQL_PROGRAM_LINK_CMD' if program else 'YQL_UDF_LINK_CMD', '$_YQL_PREBUILT_TOOL_CMD'])
    else:
        if not program:
            unit.on_add_yql_udf_deps_contrib([])
        suppressions = (unit.get('_YQL_SOURCE_SUPPRESSIONS') or '').split()
        if suppressions:
            unit.onsuppressions(suppressions)
        unit.onsrcs((unit.get('_YQL_SOURCE_SRCS') or '').split())
        unit.onpeerdir((unit.get('_YQL_SOURCE_PEERS') or '').split())
