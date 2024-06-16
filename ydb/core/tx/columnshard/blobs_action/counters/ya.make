LIBRARY()

SRCS(
    read.cpp
    storage.cpp
    write.cpp
    remove_declare.cpp
    remove_gc.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/library/actors/core
    ydb/core/tablet_flat
)

GENERATE_ENUM_SERIALIZATION(storage.h)

END()
