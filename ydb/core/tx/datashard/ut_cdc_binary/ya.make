UNITTEST_FOR(ydb/core/tx/datashard)

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

PEERDIR(
    contrib/libs/zstd
    library/cpp/digest/md5
    library/cpp/testing/unittest
    ydb/core/testlib/default
)

SRCS(
    datashard_ut_cdc_binary.cpp
)

END()
