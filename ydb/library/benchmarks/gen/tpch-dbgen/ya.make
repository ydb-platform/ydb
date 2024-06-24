LIBRARY()

CONLYFLAGS(
    -Wno-deprecated-non-prototype
    -Wno-format
    -Wno-misleading-indentation
    -Wno-missing-field-initializers
    -Wno-string-plus-int
    -Wno-unused-but-set-variable
    -Wno-unused-parameter
    -Wno-unused-variable
    -Wno-void-pointer-to-int-cast
    -Wno-int-to-void-pointer-cast
)

IF (OS_MACOS OR OS_DARWIN)
    CONLYFLAGS(-D_POSIX_SOURCE)
ENDIF()

IF (OS_WINDOWS)
    CXXFLAGS(GLOBAL -D_POSIX_)
ENDIF()

CONLYFLAGS(GLOBAL -DVECTORWISE GLOBAL -DLINUX GLOBAL -DTPCH GLOBAL -DRNG_TEST)

SRCS(
    build.c 
    bm_utils.c 
    rnd.c 
    print.c 
    load_stub.c 
    bcd2.c
    speed_seed.c 
    text.c 
    permute.c 
    rng64.c
)

END()
