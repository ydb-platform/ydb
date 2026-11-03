DLL(kqp_scheduler_sim_api)

SRCDIR(ydb/core/kqp/runtime/scheduler/sim)

SRCS(
    api.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    library/cpp/random_provider
    library/cpp/time_provider
    ydb/core/kqp/runtime/scheduler
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
