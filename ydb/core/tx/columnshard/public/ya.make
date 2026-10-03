LIBRARY()

SRCS(
    events.cpp
)

GENERATE_ENUM_SERIALIZATION(events.h)

PEERDIR(
    ydb/core/control
    ydb/core/base
    ydb/core/protos
    ydb/core/tx
    ydb/library/actors/core
    ydb/public/api/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
