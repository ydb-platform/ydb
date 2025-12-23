LIBRARY()

SRCS(
    account_read_quoter.cpp
    quota_tracker.cpp
    read_quoter.cpp
    write_quoter.cpp
)



PEERDIR(
    ydb/core/persqueue/events
    ydb/core/persqueue/common
    ydb/core/persqueue/public/counters
    ydb/core/persqueue/pqtablet/common
)

END()

RECURSE(
)

RECURSE_FOR_TESTS(
)
