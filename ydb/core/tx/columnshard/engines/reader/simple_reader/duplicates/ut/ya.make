UNITTEST_FOR(ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates
    ydb/core/tx/columnshard/engines/reader/simple_reader
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/engines
    ydb/core/tx/columnshard
    ydb/core/grpc_services
    yql/essentials/sql/pg_dummy
    yql/essentials/core/arrow_kernels/request
    yql/essentials/udfs/common/json2
    ydb/core/base
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/testlib/default
)

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

SRCS(
)

END()
