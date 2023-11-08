LIBRARY()

SRCS(
    cookies.cpp
)

PEERDIR(
    library/cpp/digest/lower_case
    library/cpp/string_utils/scan
)

END()

RECURSE_FOR_TESTS(ut)
