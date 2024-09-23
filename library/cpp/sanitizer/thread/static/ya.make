LIBRARY()

SUBSCRIBER(g:devtools-contrib)

NO_UTIL()

PEERDIR(
    contrib/libs/clang${CLANG_VER}-rt/lib/tsan
    contrib/libs/clang${CLANG_VER}-rt/lib/tsan_cxx
)

END()

