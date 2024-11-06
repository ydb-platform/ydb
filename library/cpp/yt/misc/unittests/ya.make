GTEST(unittester-library-misc)

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    cast_ut.cpp
    enum_ut.cpp
    guid_ut.cpp
    non_null_ptr_ut.cpp
    preprocessor_ut.cpp
    strong_typedef_ut.cpp
)

PEERDIR(
    library/cpp/yt/misc
)

END()
