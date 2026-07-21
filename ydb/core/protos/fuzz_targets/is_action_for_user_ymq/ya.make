FUZZ()

SRCS(
    main.cpp
    ../../../ymq/base/action.cpp
)

PEERDIR(
    ydb/core/protos
)

CFLAGS(-Wno-deprecated-declarations)

END()
