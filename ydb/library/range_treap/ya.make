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
    ut_range_treap
)
