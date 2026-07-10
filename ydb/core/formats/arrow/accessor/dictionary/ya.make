LIBRARY()

PEERDIR(
    ydb/core/formats/arrow/accessor/abstract
    ydb/core/formats/arrow/filter
    ydb/library/formats/arrow
    ydb/library/formats/arrow/protos
)

SRCS(
    accessor.cpp
    GLOBAL constructor.cpp
    GLOBAL request.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
