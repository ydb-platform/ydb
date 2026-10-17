UNITTEST_FOR(ydb/library/yql/providers/dq/worker_manager)

SRCS(
    local_worker_manager_ut.cpp
)

PEERDIR(
    ydb/library/actors/testlib
    ydb/library/yql/providers/dq/actors
    library/cpp/testing/unittest
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
