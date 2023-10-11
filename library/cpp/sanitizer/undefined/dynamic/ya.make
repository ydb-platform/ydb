DYNAMIC_LIBRARY()

OWNER(g:devtools-contrib)

NO_UTIL()

EXPORT_ALL_DYNAMIC_SYMBOLS()

IF (CLANG16)
DYNAMIC_LIBRARY_FROM(
    contrib/libs/clang16-rt/lib/ubsan_standalone
    contrib/libs/clang16-rt/lib/ubsan_standalone_cxx
)
ELSE()
DYNAMIC_LIBRARY_FROM(
    contrib/libs/clang14-rt/lib/ubsan_standalone
    contrib/libs/clang14-rt/lib/ubsan_standalone_cxx
)
ENDIF()

END()

