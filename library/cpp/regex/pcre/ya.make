LIBRARY()

PEERDIR(
    contrib/libs/pcre
    contrib/libs/pcre/pcre16
    contrib/libs/pcre/pcre32
    library/cpp/containers/stack_array
)

SRCS(
    pcre.cpp
    regexp.cpp
)

END()

RECURSE_FOR_TESTS(
    benchmark
    ut
)

