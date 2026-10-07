YQL_LIBRARY()

SRCS(kqp_types.cpp)

PEERDIR(
    ydb/core/scheme_types
    ydb/library/mkql_proto/protos
    yql/essentials/minikql
    yql/essentials/parser/pg_wrapper/interface/type_desc
)

END()
