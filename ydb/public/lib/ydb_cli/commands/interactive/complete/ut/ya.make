UNITTEST_FOR(ydb/public/lib/ydb_cli/commands/interactive/complete)

REQUIREMENTS(cpu:1)
SRCS(
    yql_completer_ut.cpp
)

PEERDIR(
    ydb/public/lib/ydb_cli/common
)

END()
