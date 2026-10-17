LIBRARY()

SRCS(
    yql_db_scheme_resolver.h
    yql_db_scheme_resolver.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/library/actors/core
    ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/threading/future
    ydb/core/base
    ydb/core/client/minikql_compile  # gn: :db_key_resolver
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tablet
    ydb/core/tx
)

YQL_LAST_ABI_VERSION()

END()
