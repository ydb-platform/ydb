GTEST()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SIZE(MEDIUM)

SRCS(
    enum_indexed_array_ut.cpp
    expiring_set_ut.cpp
    non_empty_ut.cpp
    ordered_hash_map_ut.cpp
    sharded_set_ut.cpp
)

PEERDIR(
    library/cpp/yt/containers

    library/cpp/testing/gtest
)

END()
