UNITTEST_FOR(ydb/core/tx/columnshard/engines/storage/optimizer)

SIZE(SMALL)

PEERDIR(
    ydb/core/tx/columnshard/engines/changes
    ydb/core/tx/columnshard/engines
    ydb/core/tx/columnshard
    yql/essentials/public/udf
    ydb/core/formats/arrow/compression
    ydb/core/grpc_services
    ydb/core/scheme
    ydb/core/ydb_convert
    ydb/library/mkql_proto
    ydb/core/tx/tx_proxy
    ydb/library/mkql_proto
    ydb/core/tx/schemeshard
    yql/essentials/parser/pg_wrapper
    ydb/core/persqueue
    ydb/core/tx/time_cast
    yql/essentials/sql/pg
)

SRCS(
    ut_optimizer.cpp
)

END()
