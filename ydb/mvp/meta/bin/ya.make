PROGRAM(mvp_meta)

CFLAGS(
    -DPROFILE_MEMORY_ALLOCATIONS
)

ALLOCATOR(LF_DBG)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/mvp/meta
    library/cpp/getopt
)

YQL_LAST_ABI_VERSION()

END()
