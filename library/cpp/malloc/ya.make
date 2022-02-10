RECURSE(
    api
    api/helpers
    api/ut
    tcmalloc
    galloc
    jemalloc
    lockless
    nalf
    sample-client
    system
    mimalloc
    mimalloc/link_test
    hu
    hu/link_test
)

IF (NOT OS_WINDOWS)
    RECURSE(
        calloc
        calloc/tests
        calloc/calloc_profile_diff 
        calloc/calloc_profile_scan 
        calloc/calloc_profile_scan/ut 
    )
ENDIF()
