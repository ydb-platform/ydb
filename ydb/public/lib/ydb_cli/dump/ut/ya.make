UNITTEST_FOR(ydb/public/lib/ydb_cli/dump)

SIZE(SMALL)

SRCS(
    restore_compat_ut.cpp
    direct_external_data_source_ut.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/lib/ydb_cli/dump
)

END()
