IF (OS_LINUX OR OS_DARWIN)
    UNITTEST_FOR(yql/essentials/utils/failure_injector)

    SIZE(SMALL)

    SRCS(
        failure_injector_ut.cpp
    )

    PEERDIR(
        yql/essentials/utils/log
    )

    END()
ENDIF()
