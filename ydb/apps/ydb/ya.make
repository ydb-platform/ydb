PROGRAM(ydb)

IF(BUILD_TYPE == RELEASE)
    STRIP()
ENDIF()

IF (OS_WINDOWS)
    CFLAGS(
        -DUNICODE
        -D_UNICODE
    )
ENDIF()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/apps/ydb/commands
    ydb/core/driver_lib/gwp_asan_init
)

RESOURCE(
    ydb/apps/ydb/version.txt version.txt
)

IF (NOT USE_SSE4 AND NOT OPENSOURCE)
    # contrib/libs/glibasm can not be built without SSE4
    # Replace it with contrib/libs/asmlib which can be built this way.
    DISABLE(USE_ASMLIB)
    PEERDIR(
        contrib/libs/asmlib
    )
ENDIF()

END()

IF (OS_LINUX)
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
