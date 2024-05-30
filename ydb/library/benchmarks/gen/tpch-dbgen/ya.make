LIBRARY()
CONLYFLAGS(GLOBAL -DVECTORWISE GLOBAL -DLINUX GLOBAL -DTPCH GLOBAL -DRNG_TEST)
NO_WERROR()
IF(LINUX)
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
ENDIF()
END()
