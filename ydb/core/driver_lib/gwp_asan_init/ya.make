LIBRARY()

SRCS(
    gwp_asan_init.cpp
)

IF (CLANG AND NOT OS_WINDOWS)
    PEERDIR(
        contrib/libs/clang16-rt/lib/gwp_asan
    )
    
    # Include the optional components directly since they're in the main GWP-ASan library
    ADDINCL(
        contrib/libs/clang16-rt/lib
    )
ENDIF()

END()

RECURSE_FOR_TESTS(ut)