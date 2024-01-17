LIBRARY()

OWNER(g:devtools-contrib)

NO_UTIL()

PEERDIR(
    contrib/libs/clang${CLANG_VER}-rt/lib/ubsan_standalone
    contrib/libs/clang${CLANG_VER}-rt/lib/ubsan_standalone_cxx
)

END()

