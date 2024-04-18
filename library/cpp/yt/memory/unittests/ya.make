GTEST(unittester-library-memory)

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

IF (NOT OS_WINDOWS)
    ALLOCATOR(YT)
ENDIF()

SRCS(
    atomic_intrusive_ptr_ut.cpp
    chunked_memory_pool_ut.cpp
    chunked_memory_pool_allocator_ut.cpp
    chunked_memory_pool_output_ut.cpp
    free_list_ut.cpp
    function_view_ut.cpp
    intrusive_ptr_ut.cpp
    shared_range_ut.cpp
    weak_ptr_ut.cpp
    ref_ut.cpp
)

IF (NOT OS_WINDOWS)
    SRCS(
        safe_memory_reader_ut.cpp
    )
ENDIF()

PEERDIR(
    library/cpp/testing/gtest
    library/cpp/yt/memory
)

END()
