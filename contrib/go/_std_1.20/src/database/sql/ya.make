GO_LIBRARY()

SRCS(
    convert.go
    ctxutil.go
    sql.go
)

END()

RECURSE(
    driver
)
