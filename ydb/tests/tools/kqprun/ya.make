PROGRAM()

SRCS(
    kqprun.cpp
)

PEERDIR(
    library/cpp/getopt

    ydb/library/yql/providers/yt/gateway/file

    ydb/tests/tools/kqprun/src
)

YQL_LAST_ABI_VERSION()

END()
