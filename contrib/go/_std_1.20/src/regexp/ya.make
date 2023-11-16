GO_LIBRARY()

SRCS(
    backtrack.go
    exec.go
    onepass.go
    regexp.go
)

END()

RECURSE(
    syntax
)
