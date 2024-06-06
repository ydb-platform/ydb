GTEST()

SRCS(
    case_insensitive_string_hash.cpp
)

PEERDIR(
    library/cpp/case_insensitive_string
    library/cpp/digest/murmur
)

END()
