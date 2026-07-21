FUZZ()

CFLAGS(
    -Wno-deprecated-declarations
)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/util
)

END()
