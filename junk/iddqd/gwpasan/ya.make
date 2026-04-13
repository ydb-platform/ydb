PROGRAM()

SRCS(
    main.cpp
)
CFLAGS(
    -DGWP_ASAN_HOOKS=1
    -DSCUDO_ENABLE_HOOKS=1
)
PEERDIR(
#    ydb/core/driver_lib/gwp_asan_init
    contrib/libs/clang18-rt/lib/scudo_standalone
    contrib/libs/clang18-rt/lib/scudo_standalone_cxx
)

NO_COMPILER_WARNINGS()

ALLOCATOR(SYSTEM)

END()
