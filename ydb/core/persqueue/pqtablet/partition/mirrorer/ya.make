LIBRARY()

SRCS(
    mirrorer.cpp
)



PEERDIR(
    ydb/core/persqueue/events
    ydb/core/persqueue/common
    ydb/core/persqueue/common/proxy
    ydb/core/persqueue/pqtablet/common
    ydb/core/persqueue/public/write_meta
    ydb/public/sdk/cpp/src/client/topic/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
