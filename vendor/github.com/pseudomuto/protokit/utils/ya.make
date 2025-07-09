GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

VERSION(v0.2.0)

# Those tests do open ../fixtures, so we have to mess up with cwd a bit

DATA(
    arcadia/vendor/github.com/pseudomuto/protokit/
)

TEST_CWD(vendor/github.com/pseudomuto/protokit/fixtures)

SRCS(
    protobuf.go
    strings.go
)

GO_XTEST_SRCS(
    # protobuf_test.go
    strings_test.go
)

END()

RECURSE(
    gotest
)
