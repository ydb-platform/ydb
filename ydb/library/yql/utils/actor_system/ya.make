LIBRARY()

SRCS(
    manager.h
    manager.cpp
)

STYLE_CPP()

PEERDIR(
    ydb/library/actors/core
    ydb/library/services
    yql/essentials/providers/common/metrics
    yql/essentials/utils
)

END()
