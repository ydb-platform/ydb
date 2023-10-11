DYNAMIC_LIBRARY()

OWNER(g:devtools-contrib)

NO_UTIL()

EXPORT_ALL_DYNAMIC_SYMBOLS()

IF (CLANG16)
DYNAMIC_LIBRARY_FROM(
    contrib/libs/clang16-rt/lib/asan
    contrib/libs/clang16-rt/lib/asan_cxx
)
ELSE()
DYNAMIC_LIBRARY_FROM(
    contrib/libs/clang14-rt/lib/asan
    contrib/libs/clang14-rt/lib/asan_cxx
)
ENDIF()

END()

