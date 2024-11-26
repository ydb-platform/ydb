LIBRARY()

SRCS(
    schema_from_proto.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/yson/node
    yt/yt_proto/yt/formats
    yt/yt_proto/yt/formats
)

END()
