PROGRAM(mvp_meta)

# gn: move into parent

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
