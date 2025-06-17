GTEST(unittester-core-actions)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    actions_ut.cpp
    bind_ut.cpp
    cancelation_token_ut.cpp
    future_ut.cpp
    invoker_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core
    yt/yt/core/test_framework
    library/cpp/testing/common
    yt/yt/library/profiling
    yt/yt/library/profiling/solomon
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
