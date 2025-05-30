LIBRARY()

NO_WSHADOW()

IF (PROFILE_MEMORY_ALLOCATIONS)
    CFLAGS(-DPROFILE_MEMORY_ALLOCATIONS)
ENDIF()

IF (OS_LINUX)
    SRCS(
        mem_pool.cpp
        ctx.cpp
        rdma.cpp
        GLOBAL link_manager.cpp
    )
ENDIF()

PEERDIR(
    ydb/library/actors/interconnect/rdma/ibdrv
)

END()

RECURSE_FOR_TESTS(
    ut
)
