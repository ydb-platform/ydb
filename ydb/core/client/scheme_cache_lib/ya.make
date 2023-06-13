LIBRARY()

SRCS(
    yql_db_scheme_resolver.h
    yql_db_scheme_resolver.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/actors/core
    library/cpp/grpc/client
    library/cpp/threading/future
    ydb/core/base
    ydb/core/client/minikql_compile
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tablet
    ydb/core/tx
)

YQL_LAST_ABI_VERSION()

END()
