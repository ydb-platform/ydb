UNITTEST()

SRCS(
    scan_factory_ut.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/common/events
    ydb/core/sys_view/scan_factory
    ydb/library/actors/core
    ydb/library/actors/testlib
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
