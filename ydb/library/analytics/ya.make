LIBRARY()

PEERDIR(
    ydb/library/analytics/protos
)

SRCS(
    analytics.cpp
)

END()

RECURSE(
    protos
)
