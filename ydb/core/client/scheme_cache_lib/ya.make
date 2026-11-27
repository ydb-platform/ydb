YQL_LIBRARY()

SRCS(
    mkql_compile_service.cpp
    mkql_compile_service.h
    yql_db_scheme_resolver.h
    yql_db_scheme_resolver.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/library/actors/core
    ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/threading/future
    ydb/core/base
    ydb/core/client/minikql_compile
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tablet
    ydb/core/tx
    ydb/core/tx/scheme_board
    ydb/core/tx/scheme_cache
)

END()
