PROGRAM()

SRCS(
    main.cpp
    proxy.cpp
)

PEERDIR(
    ydb/core/base
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/discovery
    ydb/library/grpc/server
    ydb/apps/etcd_proxy/service
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/exception_policy
)

END()
