GO_LIBRARY()

SRCS(
    doc.go
    exec.go
    funcs.go
    helper.go
    option.go
    template.go
)

END()

RECURSE(
    parse
)
