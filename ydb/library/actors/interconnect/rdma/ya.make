LIBRARY()

NO_WSHADOW()

IF (PROFILE_MEMORY_ALLOCATIONS)
    CFLAGS(-DPROFILE_MEMORY_ALLOCATIONS)
ENDIF()

IF (OS_LINUX)
    SRCS(
        GLOBAL rdma_link_manager.cpp
        mem_pool.cpp
    )
ENDIF()

PEERDIR(
    ydb/library/actors/interconnect/rdma/ibdrv
)

END()

RECURSE_FOR_TESTS(
    ut
)
