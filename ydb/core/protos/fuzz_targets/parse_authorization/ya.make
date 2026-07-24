FUZZ()

PEERDIR(
    ydb/library/http_proxy/authorization
)

CFLAGS(
    -Wno-deprecated-declarations
)

SRCS(
    main.cpp
)

END()
