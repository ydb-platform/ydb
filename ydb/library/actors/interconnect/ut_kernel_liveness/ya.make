UNITTEST()

IF (OS_LINUX)

SIZE(LARGE)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

SRCS(
    tcp_user_timeout_ut.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/library/actors/interconnect/mock
    ydb/library/actors/interconnect/ut/lib
    ydb/library/actors/interconnect/ut/lib/port_manager
    ydb/library/actors/interconnect/ut/protos
    library/cpp/testing/unittest
)

ENDIF()

END()
