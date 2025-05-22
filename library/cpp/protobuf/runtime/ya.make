LIBRARY()

IF (USE_VANILLA_PROTOC)
    PEERDIR(contrib/libs/protobuf_std)
ELSE()
    PEERDIR(contrib/libs/protobuf)
ENDIF()

END()
