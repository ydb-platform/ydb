LIBRARY()

SRCS(
    manager.h
    manager.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/services
    yql/essentials/providers/common/metrics
    yql/essentials/utils
)

END()

RECURSE_FOR_TESTS(
    style
)
