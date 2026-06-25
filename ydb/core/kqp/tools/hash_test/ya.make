PROGRAM()

YQL_LAST_ABI_VERSION()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()

PEERDIR(
    library/cpp/testing/unittest

    ydb/library/yql/dq/comp_nodes
    ydb/library/yql/dq/comp_nodes/ut/utils
)

IF (ARCH_X86_64)

CFLAGS(
    -mprfchw
)

ENDIF()

SRCS(
    main.cpp
)

END()
