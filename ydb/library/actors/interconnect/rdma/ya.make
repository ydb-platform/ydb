
# We need configured soft RoCE or RDMA compatible hardware to perfform rdma tests.
# Run it only if explicitly enabled via -DTEST_ICRDMA=1
IF(TEST_ICRDMA)
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
