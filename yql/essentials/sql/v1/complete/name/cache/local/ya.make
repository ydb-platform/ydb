LIBRARY()

SRCS(
    cache.cpp
)

PEERDIR(
    yql/essentials/sql/v1/complete/name/cache
    library/cpp/cache
    library/cpp/time_provider
)

END()

RECURSE_FOR_TESTS(
    ut
)
