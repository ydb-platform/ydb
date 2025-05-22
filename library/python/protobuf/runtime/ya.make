PY23_LIBRARY()

IF (USE_VANILLA_PROTOC AND NOT PYTHON2)
    PEERDIR(
        contrib/python/protobuf_std
    )
ELSE()
    PEERDIR(
        contrib/python/protobuf
    )
ENDIF()

END()
