GO_LIBRARY()

SRCS(
    cmd.go
    config.go 
    doc.go
    launcher.go
    service_connector.go
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
