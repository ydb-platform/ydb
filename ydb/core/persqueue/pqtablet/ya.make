LIBRARY()

SRCS(
    metering_sink.cpp
    pq_impl.cpp
    pq_impl_app.cpp
    pq_impl_app_sendreadset.cpp
    transaction.cpp
)



PEERDIR(
    ydb/core/persqueue/pqtablet/common
    ydb/core/persqueue/common/proxy
    ydb/core/persqueue/public/counters
    ydb/core/persqueue/pqtablet/cache
    ydb/core/persqueue/pqtablet/partition
)

END()

RECURSE(
    blob
    common
    partition
)

RECURSE_FOR_TESTS(
)
