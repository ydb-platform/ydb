UNITTEST_FOR(ydb/core/grpc_services/validation)

OWNER(
    ilnaz
    g:kikimr
)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/grpc_services/validation/ut/protos
)

SRCS(
    ut.cpp
)

END()
