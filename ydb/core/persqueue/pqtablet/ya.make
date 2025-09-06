LIBRARY()

SRCS(
    pq_impl.cpp
    pq_impl_app.cpp
    pq_impl_app_sendreadset.cpp
)



PEERDIR(
    contrib/libs/fmt
    ydb/library/actors/core
    ydb/core/base
    ydb/core/engine/minikql
    ydb/core/persqueue/events
    ydb/core/persqueue/partition_key_range
    ydb/library/logger
)

END()

RECURSE_FOR_TESTS(
#    ut
#    dread_cache_service/ut
#    ut/slow
#    ut/ut_with_sdk
)
