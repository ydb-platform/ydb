UNITTEST()

SIZE(SMALL)

PEERDIR(
    ydb/core/tx/datashard
    contrib/libs/zstd
    ydb/library/aclib
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    datashard_ut_cdc_compression.cpp
)

END()
