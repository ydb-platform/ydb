GO_LIBRARY()

SRCS(
    consts.go
    consts_norace.go
    intrinsics.go
    nih.go
    sys.go
    zversion.go
)

GO_XTEST_SRCS(intrinsics_test.go)

END()

RECURSE(
)
