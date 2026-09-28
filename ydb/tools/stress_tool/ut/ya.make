IF (SANITIZER_TYPE AND AUTOCHECK)

ELSE()

UNITTEST_FOR(ydb/tools/stress_tool/lib)

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

SRCS(
    ../device_test_tool_ut.cpp
    device_test_tool_cli_ut.cpp
)

PEERDIR(
    ydb/apps/version
    ydb/core/load_test/ddisk
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
    yql/essentials/minikql/comp_nodes/llvm16
    yt/yql/providers/yt/comp_nodes/dq/llvm16
    yt/yql/providers/yt/comp_nodes/llvm16
)

END()
ENDIF()
