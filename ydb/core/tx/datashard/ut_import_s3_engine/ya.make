UNITTEST_FOR(ydb/core/tx/datashard)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/zstd
    ydb/core/testlib
    ydb/core/tx
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
)

YQL_LAST_ABI_VERSION()

SRCS(
    import_s3_engine_ut.cpp
)

END()
