FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/discovery
    ydb/core/protos
    ydb/public/api/protos
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

END()
