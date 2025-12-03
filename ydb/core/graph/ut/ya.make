UNITTEST_FOR(ydb/core/graph)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ELSE()
    SIZE(SMALL)
ENDIF()

SRC(
    graph_ut.cpp
)

PEERDIR(
    ydb/library/actors/helpers
    ydb/core/tx/schemeshard/ut_helpers
    ydb/core/testlib/default
    ydb/core/graph/shard
    ydb/core/graph/service
)

YQL_LAST_ABI_VERSION()

END()
