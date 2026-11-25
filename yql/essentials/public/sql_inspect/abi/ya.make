YQL_INTERNALIZED_LIBRARY(sql_inspect_abi)

IF (NOT OPENSOURCE AND NOT BUILD_YQL_FROM_SOURCES)
    INCLUDE(${ARCADIA_ROOT}/yql/resources.inc)
ENDIF()

PEERDIR(
    yql/essentials/public/sql_inspect/abi/impl
)

END()

RECURSE(
    interface
    protos
    serialization
)
