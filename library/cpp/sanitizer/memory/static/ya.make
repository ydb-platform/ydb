LIBRARY()

NO_UTIL()

PEERDIR(
    contrib/libs/clang${COMPILER_VERSION}-rt/lib/msan
    contrib/libs/clang${COMPILER_VERSION}-rt/lib/msan_cxx
)

END()

