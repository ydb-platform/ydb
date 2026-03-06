LIBRARY()

PEERDIR(
    ydb/core/formats/arrow/accessor/abstract
    ydb/core/tx/columnshard/engines/protos
    ydb/library/formats/arrow
    ydb/library/formats/arrow/protos
)

SRCS(
    accessor.cpp
    additional_data.cpp
    GLOBAL constructor.cpp
    GLOBAL request.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
