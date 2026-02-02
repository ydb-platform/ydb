UNITTEST_FOR(ydb/mvp/core)

SIZE(SMALL)

SRCS(
    mvp_ut.cpp
    mvp_tokens.cpp
    mvp_test_runtime.cpp
<<<<<<< HEAD
    mvp_security_printer_ut.cpp
=======
    proto_masking_ut.cpp
>>>>>>> 449f1fe4afc (SecureShortDebugStringMasked)
)

PEERDIR(
    ydb/core/testlib/actors
    contrib/libs/jwt-cpp
    ydb/library/testlib/service_mocks
    ydb/mvp/core/ut/protos
)

END()
