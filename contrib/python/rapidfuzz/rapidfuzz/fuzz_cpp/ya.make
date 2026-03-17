PY3_LIBRARY()

VERSION(3.14.3)

LICENSE(
    BSL-1.0 AND
    MIT AND
    MPL-2.0 AND
    NCSA
)

PEERDIR(
    contrib/python/numpy
)

ADDINCL(
    contrib/python/rapidfuzz/extern/rapidfuzz-cpp
    contrib/python/rapidfuzz/extern/taskflow
    contrib/python/rapidfuzz/rapidfuzz
    contrib/python/rapidfuzz/rapidfuzz/distance
    FOR
    cython
    contrib/python/rapidfuzz
    FOR
    cython
    contrib/python/rapidfuzz/rapidfuzz
    FOR
    cython
    contrib/python/rapidfuzz/rapidfuzz
    FOR
    cython
    contrib/python/rapidfuzz/rapidfuzz/distance
)

NO_COMPILER_WARNINGS()

NO_LINT()



PY_SRCS(
    NAMESPACE rapidfuzz
    CYTHON_CPP
    fuzz_cpp.pyx
)

END()
