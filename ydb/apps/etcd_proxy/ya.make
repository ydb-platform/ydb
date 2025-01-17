PROGRAM()

SRCS(
    main.cpp
    proxy.cpp
)

PEERDIR(
    ydb/core/base
    ydb/public/api/grpc
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_discovery
    ydb/public/sdk/cpp/client/ydb_query
    ydb/library/grpc/server
    ydb/apps/etcd_proxy/service
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/exception_policy
)

RESOURCE(
    ydb/apps/etcd_proxy/create.sql create.sql
)

END()
