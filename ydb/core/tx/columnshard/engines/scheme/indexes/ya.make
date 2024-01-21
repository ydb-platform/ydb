LIBRARY()

SRCS(
    abstract.cpp
    GLOBAL bloom.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/formats/arrow
)

END()
