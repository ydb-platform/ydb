LIBRARY()

SRCS(
    readproxy.cpp
)



PEERDIR(
    ydb/core/persqueue/events
    ydb/core/persqueue/common
    ydb/core/persqueue/pqtablet/batching
)

END()

RECURSE_FOR_TESTS(
)
