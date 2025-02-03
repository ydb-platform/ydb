IF (SANITIZER_TYPE AND AUTOCHECK)

ELSE()

UNITTEST_FOR(ydb/tools/stress_tool/lib)

SIZE(LARGE)
TAG(ya:fat)

SRC(
    ../device_test_tool_ut.cpp
)

PEERDIR(
    ydb/apps/version
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
    yql/essentials/minikql/comp_nodes/llvm14
)

END()
ENDIF()
