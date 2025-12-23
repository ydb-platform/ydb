LIBRARY()

PEERDIR(
    library/cpp/string_utils/base64
)

SRCS(
    hash_types.cpp
    hashes_checker.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
