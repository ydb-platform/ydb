FUZZ()
CFLAGS(
    -Wno-deprecated-declarations
)

PEERDIR(
    ydb/core/protos
    library/cpp/protobuf/json
)

SRCS(
    main.cpp
)

END()
