LIBRARY()

PEERDIR(
    ydb/core/formats/arrow/protos
    ydb/core/formats/arrow/accessor/abstract
)

SRCS(
    accessor.cpp
    GLOBAL constructor.cpp
    GLOBAL request.cpp
)

END()
