GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    address.pb.go
    address.pb.validate.go
    backoff.pb.go
    backoff.pb.validate.go
    base.pb.go
    base.pb.validate.go
    config_source.pb.go
    config_source.pb.validate.go
    event_service_config.pb.go
    event_service_config.pb.validate.go
    grpc_method_list.pb.go
    grpc_method_list.pb.validate.go
    grpc_service.pb.go
    grpc_service.pb.validate.go
    health_check.pb.go
    health_check.pb.validate.go
    http_uri.pb.go
    http_uri.pb.validate.go
    protocol.pb.go
    protocol.pb.validate.go
    socket_option.pb.go
    socket_option.pb.validate.go
)

END()
