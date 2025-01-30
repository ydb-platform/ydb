LIBRARY()

SRCS(
    yaml.cpp
    yaml.h
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/yaml/as
    ydb/core/viewer/protos
)

END()
