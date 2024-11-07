UNITTEST_FOR(yql/essentials/minikql/protobuf_udf)

SRCS(
    type_builder_ut.cpp
    value_builder_ut.cpp
    protobuf_ut.proto
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/lib/schema
    contrib/ydb/library/yql/providers/yt/common
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/minikql
    yql/essentials/public/udf
    contrib/ydb/library/yql/providers/common/schema/mkql
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/libs/protobuf

    #alice/wonderlogs/protos
)

YQL_LAST_ABI_VERSION()

END()
