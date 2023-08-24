GO_LIBRARY()

SRCS(
    cmd.go
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
