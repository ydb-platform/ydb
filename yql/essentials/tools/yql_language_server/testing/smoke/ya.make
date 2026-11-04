EXECTEST()

DEPENDS(
    contrib/tools/python3/bin
    yql/essentials/tools/yql_language_server
)

DATA(
    arcadia/yql/essentials/tools/yql_language_server/testing/functional/traces/session/initialization.json
)

RUN(
    NAME
    initialization
    ${ARCADIA_BUILD_ROOT}/contrib/tools/python3/bin/python3
    ${TEST_SOURCE_ROOT}/stdio.py
    ${ARCADIA_BUILD_ROOT}/yql/essentials/tools/yql_language_server/yql_language_server
    ${ARCADIA_ROOT}/yql/essentials/tools/yql_language_server/testing/functional/traces/session/initialization.json
)

END()
