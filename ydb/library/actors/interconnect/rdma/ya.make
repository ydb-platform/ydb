LIBRARY()

IF (OS_LINUX)
    SRCS(
        ctx.cpp
        ctx_open.cpp
        link_manager.cpp
        mem_pool.cpp
        rdma.cpp
        rdma.h
    )

ELSE()
    CXXFLAGS(-DMEM_POOL_DISABLE_RDMA_SUPPORT)
    SRCS(
        dummy.cpp
        mem_pool.cpp
        rdma.h
    )

ENDIF()

PEERDIR(
    contrib/libs/ibdrv
    contrib/libs/protobuf
)

END()

RECURSE(
    cq_actor
)

RECURSE_FOR_TESTS(
    ut
)
