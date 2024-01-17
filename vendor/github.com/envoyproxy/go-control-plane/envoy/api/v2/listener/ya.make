GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    listener.pb.go
    listener.pb.validate.go
    listener_components.pb.go
    listener_components.pb.validate.go
    quic_config.pb.go
    quic_config.pb.validate.go
    udp_listener_config.pb.go
    udp_listener_config.pb.validate.go
)

END()
