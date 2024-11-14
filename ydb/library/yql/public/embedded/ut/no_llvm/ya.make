UNITTEST()

SIZE(MEDIUM)

TIMEOUT(300)

PEERDIR(
    yql/essentials/public/udf/service/exception_policy
    ydb/library/yql/public/embedded/no_llvm
)

YQL_LAST_ABI_VERSION()

SRCDIR(
    ydb/library/yql/public/embedded/ut
)

SRCS(
    yql_embedded_ut.cpp
)

END()

