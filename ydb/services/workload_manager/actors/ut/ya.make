UNITTEST_FOR(ydb/services/workload_manager/actors)

SIZE(SMALL)

SRCS(
    resource_pool_tracker_ut.cpp
)

PEERDIR(
    ydb/services/workload_manager/service

    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
