GTEST(unittester-core-bus-tcp)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

EXPLICIT_DATA()

PROTO_NAMESPACE(yt)

SRCS(
    bus_ut.cpp
    ssl_ut.cpp
    tcp_specific_ut.cpp
    traits.cpp
)

IF (YT_CUSTOM_INTERNAL_BUILD)
    SRCS(
        ssl2_ut.cpp
    )
    INCLUDE(${ARCADIA_ROOT}/yt/yt/core/bus/tcp/unittests/yt_custom_internal_testdata/yt_custom_internal.inc)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core
    yt/yt/core/test_framework
    yt/yt/core/bus/unittests/lib
    library/cpp/testing/common
    library/cpp/resource
)

REQUIREMENTS(
    cpu:4
    ram:4
    ram_disk:1
)

FORK_TESTS()

SPLIT_FACTOR(5)

SIZE(MEDIUM)

IF (OS_DARWIN)
    SIZE(LARGE)
    TAG(
        ya:fat
        ya:force_sandbox
        ya:exotic_platform
        ya:large_tests_on_single_slots
    )
ENDIF()

END()
