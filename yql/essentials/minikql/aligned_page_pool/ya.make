LIBRARY()

SRCDIR(yql/essentials/minikql)

SRCS(
    aligned_page_pool.cpp
    aligned_page_pool.h
    fake_mmap.cpp
    fake_mmap.h
    global_page_pool.h
    global_pools.h
    page_pool_constants.h
    system_mmap.cpp
    system_mmap.h
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    yql/essentials/public/udf/sanitizer_utils
    yql/essentials/utils
)

END()
