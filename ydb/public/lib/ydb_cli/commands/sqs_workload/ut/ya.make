UNITTEST_FOR(ydb/public/lib/ydb_cli/commands/sqs_workload)

REQUIREMENTS(cpu:1)
SRCS(
    sqs_workload_scenario_ut.cpp
)

PEERDIR(
    ydb/public/lib/ydb_cli/commands
    ydb/public/lib/ydb_cli/commands/sqs_workload
)

END()
