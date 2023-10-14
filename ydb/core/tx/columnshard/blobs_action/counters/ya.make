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
    library/cpp/actors/core
    ydb/core/tablet_flat
)

END()
