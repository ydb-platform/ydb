LIBRARY()

PEERDIR(
    library/cpp/yt/misc
)

SRCS(
    ensure.cpp
    function.cpp
    hash.cpp
    maybe.cpp
    out.cpp
    preprocessor.cpp
    reflection.cpp
    small_string.cpp
    struct.cpp
)

END()

RECURSE_FOR_TESTS(
    benchmark
    ut
)
