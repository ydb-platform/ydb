GO_LIBRARY()

SRCS(
    cmd.go
    config.go
    data_source_collection.go
    doc.go
    grpc_metrics.go
    httppuller.go
    launcher.go
    service_connector.go
    service_metrics.go
    service_pprof.go
    validate.go
)

END()

RECURSE(
    datasource
    paging
    streaming
    utils
)
