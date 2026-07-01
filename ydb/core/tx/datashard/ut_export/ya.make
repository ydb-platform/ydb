UNITTEST_FOR(ydb/core/tx/datashard)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    backup_restore_traits_ut.cpp
    export_parquet_ut.cpp
    export_s3_buffer_ut.cpp
)

GENERATE_ENUM_SERIALIZATION(ydb/core/tx/datashard/backup_restore_traits.h)
GENERATE_ENUM_SERIALIZATION(ydb/core/tx/datashard/ut_export/export_parquet_ut_enums.h)

END()
