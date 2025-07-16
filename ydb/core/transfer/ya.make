LIBRARY()

PEERDIR(
    ydb/core/tx/replication/service
)

SRCS(
    transfer_writer.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    ut/common
)

RECURSE_FOR_TESTS(
    ut/functional
    ut/large
)

