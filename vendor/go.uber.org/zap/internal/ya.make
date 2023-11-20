GO_LIBRARY()

LICENSE(MIT)

SRCS(
    level_enabler.go
)

END()

RECURSE(
    bufferpool
    color
    exit
    pool
    readme
    stacktrace
    ztest
)
