GTEST(unittester-small-containers)

OWNER(g:yt)

SRCS(
    compact_flat_map_ut.cpp
    compact_heap_ut.cpp
    compact_set_ut.cpp
    compact_vector_ut.cpp
)

PEERDIR(
    library/cpp/yt/small_containers
    library/cpp/testing/gtest
)

END()
