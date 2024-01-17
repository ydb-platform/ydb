GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    benchmark_service.pb.go
    benchmark_service_grpc.pb.go
    control.pb.go
    empty.pb.go
    messages.pb.go
    payloads.pb.go
    report_qps_scenario_service.pb.go
    report_qps_scenario_service_grpc.pb.go
    stats.pb.go
    test.pb.go
    test_grpc.pb.go
    worker_service.pb.go
    worker_service_grpc.pb.go
)

END()

RECURSE(core)
