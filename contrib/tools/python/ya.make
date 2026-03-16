PROGRAM(python)

LICENSE(PSF-2.0)

VERSION(2.7.18)

ORIGINAL_SOURCE(https://github.com/python/cpython)

PEERDIR(
    contrib/tools/python/libpython
    contrib/tools/python/src/Modules/_sqlite
)

END()

RECURSE_FOR_TESTS(
    tests
)
