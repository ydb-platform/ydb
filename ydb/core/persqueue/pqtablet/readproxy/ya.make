LIBRARY()

SRCS(
    readproxy.cpp
)



PEERDIR(
    ydb/core/persqueue/events
    ydb/core/persqueue/common
)

END()

RECURSE_FOR_TESTS(
)
