OWNER(g:yt)

IF (NOT OS_DARWIN AND NOT SANITIZER_TYPE)
    SET(YT_ALLOC_ENABLED yes)
ENDIF()

RECURSE(
    api
    impl
)

IF (YT_ALLOC_ENABLED)
    RECURSE(
        ut
        benchmarks
    )
ENDIF()
