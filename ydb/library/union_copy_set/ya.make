LIBRARY()

PEERDIR(
    library/cpp/containers/stack_vector
)

SRCS(
    union_copy_set.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
