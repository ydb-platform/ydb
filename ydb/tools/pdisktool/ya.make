PROGRAM(pdisktool)

IF (OS_LINUX)
    # Compare mold with LLD without a GDB index on the same inputs.
    SET(_LINKER_ID mold)
    LDFLAGS(
        -Wl,--no-gdb-index
        -Wl,--no-rosegment
        -Wl,--build-id=sha1
        -Wl,--perf
    )
    # Preserve the non-PIE flag normally added only for LLD.
    IF (NOT PIE)
        IF (NOT PIC OR PIC_NO_PIE)
            LDFLAGS(-Wl,-no-pie)
        ENDIF()
    ENDIF()
ENDIF()

PEERDIR(
    library/cpp/getopt
    ydb/apps/version
    ydb/tools/pdisktool/lib
)

SRCS(
    main.cpp
)

END()

RECURSE(
    lib
    proto
)

RECURSE_FOR_TESTS(
    ut
)
