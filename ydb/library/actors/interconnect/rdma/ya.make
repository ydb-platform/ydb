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

    PEERDIR(
        ydb/library/actors/interconnect/address
        ydb/library/actors/util
        contrib/libs/ibdrv
        contrib/libs/protobuf
        library/cpp/monlib/dynamic_counters
    )

ELSE()
    CXXFLAGS(-DMEM_POOL_DISABLE_RDMA_SUPPORT)
    SRCS(
        dummy.cpp
        mem_pool.cpp
        rdma.h
    )

    PEERDIR(
        ydb/library/actors/interconnect/address
        ydb/library/actors/util
        contrib/libs/protobuf
        library/cpp/monlib/dynamic_counters
    )

ENDIF()

END()

RECURSE(
    cq_actor
)

RECURSE_FOR_TESTS(
    ut
    ut_mem_pool_limit
)
