DYNAMIC_LIBRARY()

SUBSCRIBER(g:devtools-contrib)

NO_UTIL()

EXPORT_ALL_DYNAMIC_SYMBOLS()

DYNAMIC_LIBRARY_FROM(
    contrib/libs/clang${CLANG_VER}-rt/lib/tsan
    contrib/libs/clang${CLANG_VER}-rt/lib/tsan_cxx
)

END()

