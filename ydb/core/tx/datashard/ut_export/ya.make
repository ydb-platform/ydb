UNITTEST_FOR(ydb/core/tx/datashard)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    backup_restore_traits_ut.cpp
    export_fs_parquet_ut.cpp
    export_s3_buffer_ut.cpp
)

GENERATE_ENUM_SERIALIZATION(export_enums.h)

END()
