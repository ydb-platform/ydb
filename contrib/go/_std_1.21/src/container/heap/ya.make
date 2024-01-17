GO_LIBRARY()

SRCS(
    heap.go
)

GO_TEST_SRCS(heap_test.go)

GO_XTEST_SRCS(
    example_intheap_test.go
    example_pq_test.go
)

END()

RECURSE(
)
