UNITTEST_FOR(ydb/core/tx/datashard)

SIZE(SMALL)

PEERDIR(
    contrib/libs/zstd
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    datashard_ut_cdc_compression.cpp
)

END()
