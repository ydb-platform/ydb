UNITTEST_FOR(contrib/deprecated/bdb)

WITHOUT_LICENSE_TEXTS()

PEERDIR(
    library/cpp/digest/old_crc
)

SRCS(
    bdb_ut.cpp
)

END()
