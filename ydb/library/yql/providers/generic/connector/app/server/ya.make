GO_LIBRARY()

SRCS(
    cmd.go
    doc.go
    config.go 
    server.go
    validate.go
)

END()

RECURSE(
    clickhouse
    postgresql
    rdbms
    utils
)
