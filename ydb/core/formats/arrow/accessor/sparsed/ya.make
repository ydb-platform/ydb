LIBRARY()

PEERDIR(
    ydb/core/formats/arrow/protos
    ydb/core/formats/arrow/accessor/abstract
)

SRCS(
    GLOBAL constructor.cpp
    GLOBAL request.cpp
    accessor.cpp
)

END()
