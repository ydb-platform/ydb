UNITTEST_FOR(ydb/public/lib/ydb_cli/import)

SRCS(
    parquet_progress_ut.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
)

END()
