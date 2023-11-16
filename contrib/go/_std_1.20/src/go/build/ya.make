GO_LIBRARY()

SRCS(
    build.go
    doc.go
    gc.go
    read.go
    syslist.go
    zcgo.go
)

END()

RECURSE(
    constraint
)
