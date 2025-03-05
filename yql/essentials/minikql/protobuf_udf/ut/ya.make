IF (NOT OPENSOURCE)

UNITTEST_FOR(yql/essentials/minikql/protobuf_udf)

SRCS(
    type_builder_ut.cpp
    value_builder_ut.cpp
    protobuf_ut.proto
)

PEERDIR(
    yt/yql/providers/yt/lib/schema
    yt/yql/providers/yt/common
    yt/yql/providers/yt/codec
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/minikql
    yql/essentials/public/udf
    yql/essentials/providers/common/schema/mkql
    yql/essentials/providers/common/codec
    yql/essentials/sql
    yql/essentials/sql/pg_dummy
    contrib/libs/protobuf

    #alice/wonderlogs/protos
)

YQL_LAST_ABI_VERSION()

END()

ENDIF()

