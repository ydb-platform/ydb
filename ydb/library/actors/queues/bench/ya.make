PROGRAM(bench)

IF (OS_LINUX)
    ALLOCATOR(TCMALLOC_256K)
ENDIF()

SRCS(
    main.cpp

    probes.cpp
    queue_tracer.cpp
)

PEERDIR(
    ydb/library/actors/queues/observer
    library/cpp/getopt
    library/cpp/lwtrace
    library/cpp/lwtrace/mon
)

END()
