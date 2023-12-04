LIBRARY()

OWNER(g:devtools-contrib)

NO_UTIL()

PEERDIR(
    contrib/libs/clang${YA_CLANG_VER}-rt/lib/msan
    contrib/libs/clang${YA_CLANG_VER}-rt/lib/msan_cxx
)

END()

