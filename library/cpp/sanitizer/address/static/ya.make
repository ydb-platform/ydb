LIBRARY()

OWNER(g:devtools-contrib)

NO_UTIL()

PEERDIR(
    contrib/libs/clang${YA_CLANG_VER}-rt/lib/asan
    contrib/libs/clang${YA_CLANG_VER}-rt/lib/asan_cxx
    contrib/libs/clang${YA_CLANG_VER}-rt/lib/asan_static
)

END()

