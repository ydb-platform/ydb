GTEST(unittester-library-misc)

OWNER(g:yt)

SRCS(
    enum_ut.cpp
    guid_ut.cpp
    farm_fingerprint_stability_ut.cpp
    preprocessor_ut.cpp
)

PEERDIR(
    library/cpp/yt/misc
)

END()
