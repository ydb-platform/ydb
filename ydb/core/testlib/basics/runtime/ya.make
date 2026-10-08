LIBRARY()

SRCS(
    runtime.cpp
    runtime.h
    tablet_helpers.cpp
    tablet_helpers.h
)

PEERDIR(
    ydb/core/base
    ydb/core/mind/dynamic_nameserver
    ydb/core/tablet
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
