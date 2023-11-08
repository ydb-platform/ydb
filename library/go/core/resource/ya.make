GO_LIBRARY()

SRCS(resource.go)

END()

RECURSE(
    cc
    test
    test-bin
    test-fileonly
    test-files
    test-keyonly
)
