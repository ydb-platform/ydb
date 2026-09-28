LIBRARY()

SRCS(
    base_schematized_log_writer.cpp
    column_shard_log_writer.cpp
)

PEERDIR(
    ydb/core/testlib
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/core/protos
    yql/essentials/sql/pg_dummy
    ydb/core/tx/columnshard/hooks/testing
    ydb/core/tx/columnshard/test_helper
    ydb/core/tx/columnshard
    ydb/core/kqp/ut/olap/helpers
    ydb/core/kqp/ut/olap/combinatory
    ydb/core/wrappers
)


YQL_LAST_ABI_VERSION()

END()
