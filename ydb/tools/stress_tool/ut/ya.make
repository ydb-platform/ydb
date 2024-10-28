IF (SANITIZER_TYPE AND AUTOCHECK)

ELSE()

UNITTEST_FOR(ydb/tools/stress_tool/lib)

SIZE(LARGE)
TIMEOUT(3600)
TAG(ya:fat)

SRC(
    ../device_test_tool_ut.cpp
)

PEERDIR(
    ydb/apps/version
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/sql/pg
    ydb/library/yql/minikql/comp_nodes/llvm14
)

END()
ENDIF()
