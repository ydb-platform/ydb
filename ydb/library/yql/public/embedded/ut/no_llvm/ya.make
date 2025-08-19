UNITTEST()

TAG(ya:manual)

SIZE(MEDIUM)

TIMEOUT(300)

PEERDIR(
    ydb/library/yql/public/udf/service/exception_policy
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

