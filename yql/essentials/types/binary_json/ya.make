LIBRARY()

PEERDIR(
    library/cpp/containers/absl
    library/cpp/json
    contrib/libs/simdjson
)

SRCS(
    format.cpp
    read.cpp
    write.cpp
    write_internal.cpp
)

GENERATE_ENUM_SERIALIZATION(format.h)

CFLAGS(
    -Wno-assume
)

END()

RECURSE(
    dom
)

RECURSE_FOR_TESTS(
    ut
    ut_benchmark
)
