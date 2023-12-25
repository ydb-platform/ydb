LIBRARY()

PEERDIR(
    ydb/library/mkql_proto/protos
    ydb/library/yql/minikql/computation
    ydb/library/yql/parser/pg_catalog
    ydb/library/yql/providers/common/codec
    ydb/public/api/protos
)

SRCS(
    mkql_proto.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
