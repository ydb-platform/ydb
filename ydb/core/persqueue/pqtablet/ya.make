LIBRARY()

SRCS(
    pq_impl.cpp
    pq_impl_app.cpp
    pq_impl_app_sendreadset.cpp
    transaction.cpp
)



PEERDIR(
    ydb/core/persqueue/pqtablet/common
    ydb/core/persqueue/pqtablet/partition
)

END()

RECURSE(
    common
    partition
)

RECURSE_FOR_TESTS(
#    ut
#    dread_cache_service/ut
#    ut/slow
#    ut/ut_with_sdk
)
