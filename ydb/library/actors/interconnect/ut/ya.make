UNITTEST()

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(1200)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

SRCS(
    channel_scheduler_ut.cpp
    event_holder_pool_ut.cpp
    interconnect_ut.cpp
    large.cpp
    outgoing_stream_ut.cpp
    poller_actor_ut.cpp
    dynamic_proxy_ut.cpp
    sticking_ut.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/library/actors/interconnect/ut/lib
    ydb/library/actors/interconnect/ut/protos
    ydb/library/actors/testlib
    library/cpp/digest/md5
    library/cpp/testing/unittest
)

END()
