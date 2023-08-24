GO_PROGRAM(yq-connector)

SRCS(main.go)

END()

RECURSE(
    client
    config
    server
)
