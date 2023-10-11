LIBRARY()

OWNER(g:devtools-contrib)

NO_UTIL()

IF (CLANG16)
    PEERDIR(
        contrib/libs/clang16-rt/lib/ubsan_standalone
        contrib/libs/clang16-rt/lib/ubsan_standalone_cxx
    )
ELSE()
    PEERDIR(
        contrib/libs/clang14-rt/lib/ubsan_standalone
        contrib/libs/clang14-rt/lib/ubsan_standalone_cxx
    )
ENDIF()

END()

