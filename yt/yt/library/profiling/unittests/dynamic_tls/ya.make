GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    profiling_dynamic_tls_ut.cpp
)

PEERDIR(
    library/cpp/testing/common
    library/cpp/testing/gtest
)

DEPENDS(
    yt/yt/library/profiling/unittests/dynamic_tls/shared
)

ALLOCATOR(SYSTEM)

END()

RECURSE(
    shared
)
