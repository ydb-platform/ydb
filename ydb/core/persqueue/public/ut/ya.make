UNITTEST()

SIZE(SMALL)

SRCS(
    metrics_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/persqueue/public
)

YQL_LAST_ABI_VERSION()

END()
