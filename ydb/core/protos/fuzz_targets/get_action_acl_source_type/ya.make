FUZZ()

SRCS(
    main.cpp
    ../../../ymq/base/acl.cpp
)

PEERDIR(
    ydb/library/aclib
)

CFLAGS(-Wno-deprecated-declarations)

END()
