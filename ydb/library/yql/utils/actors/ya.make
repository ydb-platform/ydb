LIBRARY()

SRCS(
    rich_actor.cpp
    http_sender_actor.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/http
    library/cpp/retry
)

END()

RECURSE_FOR_TESTS(
    ut
)
