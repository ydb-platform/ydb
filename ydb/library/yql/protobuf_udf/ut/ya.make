UNITTEST_FOR(ydb/library/yql/protobuf_udf)

SRCS(
    type_builder_ut.cpp
    protobuf_ut.proto
)

PEERDIR(
    ydb/library/yql/providers/yt/lib/schema
    ydb/library/yql/providers/yt/common
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/minikql
    ydb/library/yql/public/udf
    ydb/library/yql/providers/common/schema/mkql
    contrib/libs/protobuf

    #alice/wonderlogs/protos
)

YQL_LAST_ABI_VERSION()

END()
