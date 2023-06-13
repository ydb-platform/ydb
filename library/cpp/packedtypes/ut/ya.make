UNITTEST_FOR(library/cpp/packedtypes)

PEERDIR(
    library/cpp/digest/old_crc
)

SRCS(
    longs_ut.cpp
    packed_ut.cpp
    packedfloat_ut.cpp
    zigzag_ut.cpp
)

END()
