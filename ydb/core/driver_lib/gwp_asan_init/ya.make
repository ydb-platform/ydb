LIBRARY()

SRCS(
    gwp_asan_init.cpp
)

IF (CLANG AND NOT OS_WINDOWS)
    PEERDIR(
        contrib/libs/clang18-rt/lib/scudo_standalone
    )
    
    # Include the optional components directly since they're in the main GWP-ASan library
    ADDINCL(
        contrib/libs/clang18-rt/lib
    )
ENDIF()

END()

RECURSE_FOR_TESTS(ut)
