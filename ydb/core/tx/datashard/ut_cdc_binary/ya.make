UNITTEST_FOR(ydb/core/tx/datashard)

SIZE(SMALL)

PEERDIR(
    contrib/libs/zstd
    library/cpp/digest/md5
    library/cpp/testing/unittest
)

SRCS(
    datashard_ut_cdc_binary.cpp
)

END()
