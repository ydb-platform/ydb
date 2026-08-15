GTEST(unittester-library-misc)

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    cast_ut.cpp
    compare_ut.cpp
    enum_ut.cpp
    guid_ut.cpp
    hash_ut.cpp
    numeric_helpers_ut.cpp
    preprocessor_ut.cpp
    range_helpers_ut.cpp
    strong_typedef_ut.cpp
    typeid_sample.cpp
    typeid_ut.cpp
)

PEERDIR(
    library/cpp/yt/misc
)

END()
