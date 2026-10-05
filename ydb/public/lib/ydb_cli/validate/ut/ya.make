UNITTEST_FOR(ydb/public/lib/ydb_cli/validate)

SIZE(SMALL)

SRC(validate_ut.cpp)

PEERDIR(
    contrib/libs/zstd
    ydb/public/lib/ydb_cli/validate
)

END()
