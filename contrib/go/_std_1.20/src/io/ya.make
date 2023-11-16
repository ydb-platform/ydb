GO_LIBRARY()

SRCS(
    io.go
    multi.go
    pipe.go
)

END()

RECURSE(
    fs
    ioutil
)
