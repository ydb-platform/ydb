YQL_LIBRARY()

PEERDIR(
    ydb/library/mkql_proto/protos
    ydb/public/api/protos
    yql/essentials/minikql/computation
    yql/essentials/parser/pg_catalog
    yql/essentials/parser/pg_wrapper/interface
)

SRCS(
    mkql_proto.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
