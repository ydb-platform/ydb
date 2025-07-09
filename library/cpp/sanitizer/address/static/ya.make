LIBRARY()

NO_UTIL()

PEERDIR(
    contrib/libs/clang${COMPILER_VERSION}-rt/lib/asan
    contrib/libs/clang${COMPILER_VERSION}-rt/lib/asan_cxx
    contrib/libs/clang${COMPILER_VERSION}-rt/lib/asan_static
)

END()

