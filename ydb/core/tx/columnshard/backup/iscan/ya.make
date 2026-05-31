LIBRARY()

SRCS(
    iscan.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/tx/datashard
    ydb/library/actors/core
    ydb/library/services
    ydb/library/signals
)

END()

RECURSE_FOR_TESTS(
    ut
)