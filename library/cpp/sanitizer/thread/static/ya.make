LIBRARY()

OWNER(g:devtools-contrib)

NO_UTIL()

PEERDIR(
    contrib/libs/clang${YA_CLANG_VER}-rt/lib/tsan
    contrib/libs/clang${YA_CLANG_VER}-rt/lib/tsan_cxx
)

END()

