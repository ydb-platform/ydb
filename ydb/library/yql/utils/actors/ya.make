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

IF (NOT OPENSOURCE OR OPENSOURCE_PROJECT == "ydb")
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
