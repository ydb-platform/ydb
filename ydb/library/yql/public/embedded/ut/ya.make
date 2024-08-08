UNITTEST()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

TIMEOUT(300)

PEERDIR(
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/public/embedded
)

YQL_LAST_ABI_VERSION()

SRCS(
    yql_embedded_ut.cpp
)

END()

