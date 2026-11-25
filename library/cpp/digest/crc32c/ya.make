LIBRARY()

#!!!
PEERDIR(
    contrib/libs/crcutil
)

SRCS(
    crc32c.cpp
)

PEERDIR_FROM_POLICY(
    ALLOW
    contrib/libs/leveldb
)

END()

RECURSE_FOR_TESTS(
    ut
)
