LIBRARY()

PEERDIR(
    library/cpp/cache
    ydb/core/scheme
)

END()

RECURSE_FOR_TESTS(
    ut
)
