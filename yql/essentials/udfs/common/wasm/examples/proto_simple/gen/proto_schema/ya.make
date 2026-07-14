LIBRARY()

SRCS(
    ../../example.proto
)

IF(PROTOBUF_LITE)
    PEERDIR(
        contrib/libs/protobuf_std
    )
ELSE()
    PEERDIR(
        contrib/libs/protobuf
    )
ENDIF()

END()
