IF (OS_LINUX OR OS_DARWIN)
    UNITTEST_FOR(ydb/library/yql/utils/failure_injector)

    TAG(ya:manual)

    SIZE(SMALL)

    SRCS(
        failure_injector_ut.cpp
    )

    PEERDIR(
        ydb/library/yql/utils/log
    )

    END()
ENDIF()
