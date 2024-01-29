PROGRAM(mvp_ydbc)

OWNER(
    alexnick
    xenoxeno
    tarasov-egor
    g:kikimr
)

CFLAGS(
    -DPROFILE_MEMORY_ALLOCATIONS
)

ALLOCATOR(LF_DBG)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/mvp/ydbc
)

YQL_LAST_ABI_VERSION()

END()
