LIBRARY()

SRCS(
    reverse_accessor.cpp
    scalars.cpp
    snapshot.cpp
)

PEERDIR(
    ydb/core/protos
    contrib/libs/apache/arrow
)

END()
