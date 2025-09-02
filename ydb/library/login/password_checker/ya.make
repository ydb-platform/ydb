LIBRARY()

PEERDIR(
    library/cpp/string_utils/base64
)

SRCS(
    password_checker.cpp
    hash_checker.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
