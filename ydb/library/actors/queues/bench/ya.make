PROGRAM(bench)

IF (OS_LINUX)
    ALLOCATOR(TCMALLOC_256K)
ENDIF()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/library/actors/queues
    library/cpp/getopt
    library/cpp/lwtrace
)

END()
