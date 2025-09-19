LIBRARY()

PEERDIR(library/cpp/digest/crc32c)

END()

RECURSE_FOR_TESTS(
    ut
    exec
)