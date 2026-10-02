GTEST()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SIZE(MEDIUM)

SRCS(
    chunked_vector_ut.cpp
    default_map_ut.cpp
    enum_indexed_array_ut.cpp
    expiring_set_ut.cpp
    intrusive_linked_list_ut.cpp
    non_empty_ut.cpp
    ordered_hash_map_ut.cpp
    ring_queue_ut.cpp
    sentinel_optional_ut.cpp
    sharded_set_ut.cpp
    skip_list_ut.cpp
    slot_map_ut.cpp
    static_ring_queue_ut.cpp
    three_level_stable_vector_ut.cpp
)

PEERDIR(
    library/cpp/yt/containers
    library/cpp/yt/compact_containers
    library/cpp/yt/memory

    library/cpp/testing/gtest
)

END()
