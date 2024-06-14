LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    fixed_growth_string_output.cpp
    test_memory_tracker.cpp
    test_server_host.cpp
    test_proxy_service.cpp
    GLOBAL framework.cpp
)

PEERDIR(
    library/cpp/testing/gtest
    library/cpp/testing/hook
    yt/yt/build
    yt/yt/core
    yt/yt/core/http
    yt/yt/library/profiling/solomon
)

END()
