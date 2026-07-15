LIBRARY()

YQL_LAST_ABI_VERSION()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()

PEERDIR(
    ydb/library/yql/dq/comp_nodes
    ydb/library/yql/dq/comp_nodes/ut/utils
    library/cpp/json
)

SRCS(
    construct_join_graph.cpp
    joins.cpp
    benchmark_settings.cpp
)

END()

IF (NOT OS_WINDOWS)
    RECURSE_FOR_TESTS(
        bin
    )
ENDIF()
