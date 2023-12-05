DYNAMIC_LIBRARY()

OWNER(g:devtools-contrib)

NO_UTIL()

EXPORT_ALL_DYNAMIC_SYMBOLS()

DYNAMIC_LIBRARY_FROM(
    contrib/libs/clang${CLANG_VER}-rt/lib/asan
    contrib/libs/clang${CLANG_VER}-rt/lib/asan_cxx
)

END()

