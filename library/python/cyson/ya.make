PY23_LIBRARY()

NO_WSHADOW()

PEERDIR(
    library/c/cyson
)

SRCS(
    cyson/helpers.cpp
    cyson/unsigned_long.cpp
)

PY_SRCS(
    TOP_LEVEL
    cyson/_cyson.pyx
    cyson/__init__.py
)

END()

RECURSE(
    pymodule
)

RECURSE_FOR_TESTS(
    ut
)
