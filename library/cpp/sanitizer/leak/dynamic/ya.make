DYNAMIC_LIBRARY()

OWNER(g:devtools-contrib)

NO_UTIL()

EXPORT_ALL_DYNAMIC_SYMBOLS()

DYNAMIC_LIBRARY_FROM(
    contrib/libs/clang${YA_CLANG_VER}-rt/lib/lsan
)

END()

