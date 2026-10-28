GTEST()

SRCS(
    provider_ut.cpp
)

PEERDIR(
    ydb/library/yql/providers/qyt/provider
    yql/essentials/core
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
