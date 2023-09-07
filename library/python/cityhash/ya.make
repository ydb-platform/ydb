PY23_LIBRARY()

SRCS(
    hash.cpp
)

PY_SRCS(
    TOP_LEVEL
    cityhash.pyx
)

END()

RECURSE_FOR_TESTS(
    test
)
