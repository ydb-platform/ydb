LIBRARY()

PEERDIR(
    ydb/core/formats/arrow/accessor/abstract
    ydb/library/formats/arrow
    ydb/library/formats/arrow/protos
)

SRCS(
    GLOBAL constructor.cpp
    GLOBAL request.cpp
    accessor.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
