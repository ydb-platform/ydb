LIBRARY()

OWNER(g:devtools-contrib)

NO_UTIL()

IF (CLANG16)
PEERDIR(
    contrib/libs/clang16-rt/lib/asan
    contrib/libs/clang16-rt/lib/asan_cxx
    contrib/libs/clang16-rt/lib/asan_static
)
ELSE()
PEERDIR(
    contrib/libs/clang14-rt/lib/asan
    contrib/libs/clang14-rt/lib/asan_cxx
    contrib/libs/clang14-rt/lib/asan_static
)
ENDIF()

END()

