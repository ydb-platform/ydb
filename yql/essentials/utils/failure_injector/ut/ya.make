IF (OS_LINUX OR OS_DARWIN)
    UNITTEST_FOR(yql/essentials/utils/failure_injector)

    ENABLE(YQL_STYLE_CPP)

    SIZE(SMALL)

    SRCS(
        failure_injector_ut.cpp
    )

    PEERDIR(
        yql/essentials/utils/log
    )

    END()
ENDIF()
