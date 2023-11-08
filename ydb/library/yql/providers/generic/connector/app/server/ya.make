GO_LIBRARY()

SRCS(
    cmd.go
    config.go 
    doc.go
    grpc_metrics.go
    launcher.go
    service_connector.go
    service_metrics.go
    service_pprof.go
    validate.go
)

END()

RECURSE(
    clickhouse
    paging
    postgresql
    rdbms
    streaming
    utils
)
