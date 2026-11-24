YQL_LIBRARY()

SRCS(
    type_info.cpp
)

PEERDIR(
    ydb/core/scheme_types
    ydb/library/mkql_proto/protos
    yql/essentials/minikql
    yql/essentials/parser/pg_wrapper/interface/type_desc
)

END()
