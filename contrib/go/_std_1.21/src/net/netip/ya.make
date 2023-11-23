GO_LIBRARY()

SRCS(
    leaf_alts.go
    netip.go
    uint128.go
)

GO_TEST_SRCS(
    export_test.go
    inlining_test.go
    netip_pkg_test.go
    uint128_test.go
)

GO_XTEST_SRCS(
    fuzz_test.go
    netip_test.go
    slow_test.go
)

END()

RECURSE(
)
