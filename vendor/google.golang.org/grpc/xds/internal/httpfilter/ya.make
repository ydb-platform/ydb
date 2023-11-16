GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(httpfilter.go)

END()

RECURSE(
    fault
    rbac
    router
)
