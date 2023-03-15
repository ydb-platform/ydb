PROGRAM(library-messagebus-rain_check-test-ut)

PEERDIR(
    library/cpp/testing/unittest_main
    library/cpp/messagebus/rain_check/core
    library/cpp/messagebus/rain_check/http
    library/cpp/messagebus/rain_check/messagebus
    library/cpp/messagebus/test/helper
)

SRCS(
    ../../core/coro_ut.cpp
    ../../core/simple_ut.cpp
    ../../core/sleep_ut.cpp
    ../../core/spawn_ut.cpp
    ../../core/track_ut.cpp
    ../../http/client_ut.cpp
    ../../messagebus/messagebus_client_ut.cpp
    ../../messagebus/messagebus_server_ut.cpp
)

END()
