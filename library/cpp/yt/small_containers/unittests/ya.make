GTEST(unittester-small-containers)

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    compact_flat_map_ut.cpp
    compact_heap_ut.cpp
    compact_set_ut.cpp
    compact_vector_ut.cpp
    compact_queue_ut.cpp
)

PEERDIR(
    library/cpp/yt/small_containers
    library/cpp/testing/gtest
)

END()
