PROGRAM(mrjob)

ALLOCATOR(J)

SRCS(
    mrjob.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/ydb/library/yql/tools/exports.symlist)
ENDIF()

PEERDIR(
    yt/cpp/mapreduce/client
    ydb/library/yql/public/udf/service/terminate_policy
    ydb/library/yql/providers/common/gateway
    ydb/library/yql/utils/backtrace
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/sql/pg
    ydb/library/yql/providers/yt/job
)

YQL_LAST_ABI_VERSION()

END()
