YQL_INTERNALIZED_LIBRARY(fastcheck_abi)

IF (NOT OPENSOURCE AND NOT BUILD_YQL_FROM_SOURCES)
    INCLUDE(${ARCADIA_ROOT}/yql/resources.inc)
ENDIF()

PEERDIR(
    yql/essentials/public/fastcheck/abi/impl
)

END()

RECURSE(
    interface
    protos
    serialization
)
