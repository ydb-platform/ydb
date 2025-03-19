LIBRARY()

NO_UTIL()

PEERDIR(
    contrib/libs/clang${COMPILER_VERSION}-rt/lib/tsan
    contrib/libs/clang${COMPILER_VERSION}-rt/lib/tsan_cxx
)

END()

