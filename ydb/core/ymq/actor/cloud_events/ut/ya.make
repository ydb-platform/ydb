UNITTEST()

PEERDIR(
    ydb/core/testlib/default
    ydb/core/ymq/actor/cloud_events
)

SRCS(
    ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
