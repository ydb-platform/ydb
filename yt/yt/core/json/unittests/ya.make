GTEST(unittester-core-json)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    parser_ut.cpp
    web_json_consumer_ut.cpp
    writer_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core
    yt/yt/core/test_framework
)

REQUIREMENTS(
    cpu:4
    ram:4
    ram_disk:1
)

FORK_TESTS()

SPLIT_FACTOR(5)

SIZE(SMALL)

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
