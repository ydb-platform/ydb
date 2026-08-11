LIBRARY()

PEERDIR(
    library/cpp/yt/misc
)

SRCS(
    ensure.cpp
    function.cpp
    maybe.h
    out.cpp
    preprocessor.cpp
    reflection.cpp
    small_string.cpp
    struct.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
