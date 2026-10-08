UNITTEST_FOR(ydb/library/yql/providers/ydb/actors)

SRCS(
    read_stream_ut.cpp
    read_stream_grpc_ut.cpp
)
PEERDIR(
    contrib/libs/grpc
    library/cpp/testing/common
    library/cpp/testing/unittest
    ydb/library/yql/providers/common/ut_helpers
    ydb/public/api/grpc
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)
ADDINCL(contrib/libs/flatbuffers/include)

YQL_LAST_ABI_VERSION()
END()
