LIBRARY()

OWNER(g:devtools-contrib)

NO_UTIL()

IF (CLANG16)
PEERDIR(
    contrib/libs/clang16-rt/lib/msan
    contrib/libs/clang16-rt/lib/msan_cxx
)
ELSE()
PEERDIR(
    contrib/libs/clang14-rt/lib/msan
    contrib/libs/clang14-rt/lib/msan_cxx
)
ENDIF()

END()

