GTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

SRCS(
    otel_registry_ut.cpp
)

PEERDIR(
    library/cpp/testing/gtest
    ydb/public/sdk/cpp/plugins/metrics/otel
    ydb/public/sdk/cpp/src/client/metrics
    contrib/libs/opentelemetry-cpp
    contrib/libs/opentelemetry-cpp/api
)

END()
