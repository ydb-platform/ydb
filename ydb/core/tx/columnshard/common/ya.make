LIBRARY()

SRCS(
    reverse_accessor.cpp
    scalars.cpp
    snapshot.cpp
    portion.cpp
)

PEERDIR(
    ydb/core/protos
    contrib/libs/apache/arrow
    ydb/core/formats/arrow
)

GENERATE_ENUM_SERIALIZATION(portion.h)

END()
