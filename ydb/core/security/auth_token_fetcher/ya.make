LIBRARY()

SRCS(
    auth_token_fetcher.cpp
)

PEERDIR(
    ydb/library/actors/http
)

END()

RECURSE_FOR_TESTS(
    ut
)