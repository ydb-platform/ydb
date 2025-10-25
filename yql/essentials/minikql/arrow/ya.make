LIBRARY()

SRCS(
    arrow_util.cpp
    mkql_functions.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    yql/essentials/minikql
    yql/essentials/public/udf/arrow
)

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(-DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION)
ENDIF()

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
