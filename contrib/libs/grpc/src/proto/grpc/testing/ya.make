PROTO_LIBRARY()

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

OWNER(
    akastornov
    g:contrib
    g:cpp-contrib 
)

EXCLUDE_TAGS(
    GO_PROTO
    PY_PROTO
    PY3_PROTO
)

PROTO_NAMESPACE(
    GLOBAL
    contrib/libs/grpc
)

PEERDIR(
    contrib/libs/grpc/src/proto/grpc/core
)

GRPC()

SRCS(
    benchmark_service.proto
    compiler_test.proto
    control.proto
    echo.proto
    echo_messages.proto
    empty.proto
    empty_service.proto
    messages.proto
    metrics.proto
    payloads.proto
    proxy-service.proto
    report_qps_scenario_service.proto
    simple_messages.proto
    stats.proto
    test.proto
    worker_service.proto
)

END()
