LIBRARY()

SRCS(
    escaping.h
    nprotobuf.h
)

IF (USE_VANILLA_PROTOC)
    PEERDIR(contrib/libs/protobuf_std)
    SRCS(escaping_std.cpp)
ELSE()
    PEERDIR(contrib/libs/protobuf)
    SRCS(escaping_arcadia.cpp)
ENDIF()

END()
