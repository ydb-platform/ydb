IF (NOT SANITIZER_TYPE)
    UNITTEST_FOR(ydb/library/actors/core)

    SIZE(SMALL)

    IF (OS_WINDOWS)
        SRCS(
            mprotect_ut_win.cpp
        )
    ELSE()
        SRCS(
            mprotect_ut.cpp
        )
    ENDIF()

    END()
ENDIF()
