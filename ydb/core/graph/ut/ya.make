UNITTEST_FOR(ydb/core/graph)

REQUIREMENTS(cpu:1)
IF (SANITIZER_TYPE)
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
