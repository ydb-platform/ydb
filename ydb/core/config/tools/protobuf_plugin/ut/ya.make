UNITTEST_FOR(ydb/core/config/tools/protobuf_plugin)

FORK_SUBTESTS()

REQUIREMENTS(cpu:1)
IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/config/tools/protobuf_plugin/ut/protos
)

SRCS(
    ut.cpp
)

END()
