UNITTEST()

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(1200)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    channel_scheduler_ut.cpp
    event_holder_pool_ut.cpp
    interconnect_ut.cpp
    large.cpp
    outgoing_stream_ut.cpp
    poller_actor_ut.cpp
    dynamic_proxy_ut.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/interconnect
    library/cpp/actors/interconnect/ut/lib
    library/cpp/actors/interconnect/ut/protos
    library/cpp/actors/testlib
    library/cpp/digest/md5
    library/cpp/testing/unittest
)

END()
