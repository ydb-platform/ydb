FUZZ()

PEERDIR(
    ydb/core/protos
    library/cpp/protobuf/json
)

CFLAGS(
    -Wno-deprecated-declarations
)

SRCS(
    main.cpp
)

END()
