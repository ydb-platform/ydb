UNITTEST_FOR(ydb/core/tx/schemeshard)

SIZE(SMALL)

PEERDIR(
    ydb/core/protos
    ydb/core/tablet_flat
    ydb/core/testlib/pg
    ydb/library/actors/core
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    op_handlers_ut.cpp
)

END()
