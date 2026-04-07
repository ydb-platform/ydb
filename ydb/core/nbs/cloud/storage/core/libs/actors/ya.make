LIBRARY()

SRCS(
    helpers.cpp
    poison_pill_helper.cpp
)

PEERDIR(
    ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(ut)
