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

IF (PYTHON2)
    CYTHON_FLAGS(-EPYTHON2=1)
ELSE()
    CYTHON_FLAGS(-EPYTHON2=0)
ENDIF()

END()

RECURSE(
    pymodule
)

RECURSE_FOR_TESTS(
    ut
)
