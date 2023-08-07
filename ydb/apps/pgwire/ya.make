PROGRAM()

SRCS(
    appdata.h
    log_impl.h
    main.cpp
    pg_ydb_connection.cpp
    pg_ydb_connection.h
    pg_ydb_proxy.cpp
    pg_ydb_proxy.h
    pgwire.cpp
    pgwire.h
    signals.h
)

PEERDIR(
    ydb/core/base
    ydb/core/pgproxy
    ydb/core/local_pgwire
    ydb/core/protos
    ydb/public/api/grpc
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/draft
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/public/udf/service/exception_policy
)

END()
