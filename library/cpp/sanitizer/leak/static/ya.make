LIBRARY()

OWNER(g:devtools-contrib)

NO_UTIL()

IF (CLANG16)
PEERDIR(
    contrib/libs/clang16-rt/lib/lsan
)
ELSE()
PEERDIR(
    contrib/libs/clang14-rt/lib/lsan
)
ENDIF()

END()

