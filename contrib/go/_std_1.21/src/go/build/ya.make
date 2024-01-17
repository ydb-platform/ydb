GO_LIBRARY()

SRCS(
    build.go
    doc.go
    gc.go
    read.go
    syslist.go
    zcgo.go
)

GO_TEST_SRCS(
    build_test.go
    deps_test.go
    read_test.go
    syslist_test.go
)

END()

RECURSE(
    constraint
)
