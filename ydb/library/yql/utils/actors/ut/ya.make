IF (OS_LINUX OR OS_DARWIN)
    UNITTEST_FOR(ydb/library/yql/utils/actors)

    SIZE(SMALL)

    SRCS(
        http_sender_actor_ut.cpp
    )

    PEERDIR(
        ydb/core/testlib/basics/default
        ydb/library/yql/minikql/comp_nodes/llvm
    )

    END()
ENDIF()
