GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v1.6.0)

SRCS(
    dce.go
    doc.go
    hash.go
    marshal.go
    node.go
    node_net.go
    null.go
    sql.go
    time.go
    util.go
    uuid.go
    version1.go
    version4.go
    version6.go
    version7.go
)

GO_TEST_SRCS(
    json_test.go
    null_test.go
    seq_test.go
    sql_test.go
    uuid_test.go
)

END()

RECURSE(
    gotest
)
