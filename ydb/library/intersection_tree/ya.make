LIBRARY()

PEERDIR(
    ydb/library/union_copy_set
)

SRCS(
    intersection_tree.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
