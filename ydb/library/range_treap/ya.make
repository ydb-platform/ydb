LIBRARY()

SRCS(
    range.cpp
    range_treap.cpp
)

PEERDIR(
    util
    ydb/library/accessor
)

END()

RECURSE_FOR_TESTS(
    fuzz_targets
    ut_range_treap
)
