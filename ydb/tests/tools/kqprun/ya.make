PROGRAM()

SRCS(
    kqprun.cpp
)

PEERDIR(
    library/cpp/getopt

    ydb/tests/tools/kqprun/src
)

PEERDIR(
    ydb/library/yql/udfs/common/datetime2
    ydb/library/yql/udfs/common/string
    ydb/library/yql/udfs/common/yson2
)

YQL_LAST_ABI_VERSION()

END()
