LIBRARY()

SRCS(
    counters.cpp
    net_classifier.cpp
)

GENERATE_ENUM_SERIALIZATION(net_classifier.h)

PEERDIR(
    ydb/library/actors/core
    library/cpp/monlib/dynamic_counters
    ydb/core/base
    ydb/core/cms/console
    ydb/core/mon
    ydb/core/protos
    ydb/core/util
)

END()

RECURSE_FOR_TESTS(
    ut
)
