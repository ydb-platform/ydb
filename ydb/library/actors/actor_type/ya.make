LIBRARY()

SRCS(
    common.cpp
    indexes.cpp
    index_constructor.cpp
)

PEERDIR(
    ydb/library/actors/util
    ydb/library/actors/prof
)

GENERATE_ENUM_SERIALIZATION(common.h)

END()
