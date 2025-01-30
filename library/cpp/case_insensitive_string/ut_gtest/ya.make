GTEST()

SRCS(
    case_insensitive_string_compare.cpp
    case_insensitive_string_hash.cpp
)

PEERDIR(
    library/cpp/case_insensitive_string
    library/cpp/case_insensitive_string/ut_gtest/util
    library/cpp/digest/murmur
)

END()
