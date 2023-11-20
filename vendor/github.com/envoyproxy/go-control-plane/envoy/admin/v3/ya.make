GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    certs.pb.go
    certs.pb.validate.go
    clusters.pb.go
    clusters.pb.validate.go
    config_dump.pb.go
    config_dump.pb.validate.go
    config_dump_shared.pb.go
    config_dump_shared.pb.validate.go
    init_dump.pb.go
    init_dump.pb.validate.go
    listeners.pb.go
    listeners.pb.validate.go
    memory.pb.go
    memory.pb.validate.go
    metrics.pb.go
    metrics.pb.validate.go
    mutex_stats.pb.go
    mutex_stats.pb.validate.go
    server_info.pb.go
    server_info.pb.validate.go
    tap.pb.go
    tap.pb.validate.go
)

END()
