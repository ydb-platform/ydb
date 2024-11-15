UNITTEST()

SIZE(MEDIUM)

TIMEOUT(300)

PEERDIR(
    yql/essentials/public/udf/service/exception_policy
    ydb/library/yql/public/embedded
)

YQL_LAST_ABI_VERSION()

SRCS(
    yql_embedded_ut.cpp
)

END()

