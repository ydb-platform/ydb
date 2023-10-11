LIBRARY()

OWNER(g:devtools-contrib)

NO_UTIL()

IF (CLANG16)
PEERDIR(
    contrib/libs/clang16-rt/lib/tsan
    contrib/libs/clang16-rt/lib/tsan_cxx
)
ELSE()
PEERDIR(
    contrib/libs/clang14-rt/lib/tsan
    contrib/libs/clang14-rt/lib/tsan_cxx
)
ENDIF()

END()

