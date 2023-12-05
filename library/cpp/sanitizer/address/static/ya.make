LIBRARY()

OWNER(g:devtools-contrib)

NO_UTIL()

PEERDIR(
    contrib/libs/clang${CLANG_VER}-rt/lib/asan
    contrib/libs/clang${CLANG_VER}-rt/lib/asan_cxx
    contrib/libs/clang${CLANG_VER}-rt/lib/asan_static
)

END()

