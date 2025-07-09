LIBRARY()

SUBSCRIBER(g:devtools-contrib)

NO_UTIL()

PEERDIR(
    contrib/libs/clang${COMPILER_VERSION}-rt/lib/ubsan_standalone
    contrib/libs/clang${COMPILER_VERSION}-rt/lib/ubsan_standalone_cxx
)

END()

