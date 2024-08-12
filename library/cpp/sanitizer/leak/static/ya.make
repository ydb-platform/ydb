LIBRARY()

SUBSCRIBER(g:devtools-contrib)

NO_UTIL()

PEERDIR(
    contrib/libs/clang${CLANG_VER}-rt/lib/lsan
)

END()

