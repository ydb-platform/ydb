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
    ydb/apps/ydb_int/ydb/commands
)

IF (NOT USE_SSE4 AND NOT OPENSOURCE)
    # contrib/libs/glibcasm cannot be built without SSE4
    # Replace it with contrib/libs/asmlib which can be built this way.
    DISABLE(USE_ASMLIB)
    PEERDIR(
        contrib/libs/asmlib
    )
ENDIF()

END()
