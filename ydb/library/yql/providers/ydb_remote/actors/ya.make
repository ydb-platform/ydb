LIBRARY()

SRCS(
    dq_ydb_remote_read_actor.cpp
    read_stream.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/yql/providers/native
    ydb/library/yql/providers/ydb_remote/common
    ydb/library/yql/providers/ydb_remote/proto
    ydb/library/yql/providers/common/token_accessor/client
    ydb/public/sdk/cpp/src/client/arrow
    ydb/public/sdk/cpp/src/client/query
    yql/essentials/public/udf/arrow
)

ADDINCL(contrib/libs/flatbuffers/include)

YQL_LAST_ABI_VERSION()
END()

RECURSE_FOR_TESTS(ut)
