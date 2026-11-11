PROGRAM(pdisktool)

IF (OS_LINUX)
    # Relink with a trace to compare the cost of generating the GDB index.
    ENABLE(LINKER_TIME_TRACE)
    LDFLAGS(-Wl,--no-gdb-index)
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
