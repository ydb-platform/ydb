UNITTEST_FOR(ydb/library/yql/minikql/protobuf_udf)

TAG(ya:manual)

SRCS(
    type_builder_ut.cpp
    value_builder_ut.cpp
    protobuf_ut.proto
)

PEERDIR(
    ydb/library/yql/providers/yt/lib/schema
    ydb/library/yql/providers/yt/common
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/minikql
    ydb/library/yql/public/udf
    ydb/library/yql/providers/common/schema/mkql
    ydb/library/yql/providers/common/codec
    ydb/library/yql/sql
    ydb/library/yql/sql/pg_dummy
    contrib/libs/protobuf

    #alice/wonderlogs/protos
)

YQL_LAST_ABI_VERSION()

END()
