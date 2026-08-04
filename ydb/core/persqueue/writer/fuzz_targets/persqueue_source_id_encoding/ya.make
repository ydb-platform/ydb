FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/persqueue/writer
    ydb/core/grpc_services
    ydb/services/metadata
    ydb/services/metadata/abstract
    ydb/services/metadata/initializer
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

END()
