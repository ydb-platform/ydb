LIBRARY()

SRCS(
    runtime.cpp
    runtime.h
)

PEERDIR(
    ydb/core/base
    ydb/core/mind/dynamic_nameserver
    ydb/core/testlib/actors
    ydb/library/actors/dnsresolver
    ydb/library/actors/interconnect
)

IF (GCC)
    CFLAGS(
        -fno-devirtualize-speculatively
    )
ENDIF()

END()
