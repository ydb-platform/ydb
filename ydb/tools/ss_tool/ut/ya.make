UNITTEST_FOR(ydb/tools/ss_tool/lib)

PEERDIR(
    ydb/core/testlib/pg
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    op_inspect_ut.cpp
)

END()
