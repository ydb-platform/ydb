LIBRARY()

SRCS(
    manager.h
    manager.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/library/services
    ydb/library/yql/providers/common/metrics
    ydb/library/yql/utils
)

END()

RECURSE_FOR_TESTS(
    style
)
