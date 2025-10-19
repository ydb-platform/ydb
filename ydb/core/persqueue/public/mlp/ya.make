LIBRARY()

SRCS(
    mlp_changer.cpp
    mlp_reader.cpp
    mlp.cpp
)

PEERDIR(
    ydb/core/persqueue/events
    ydb/core/persqueue/public
    ydb/core/persqueue/public/describer
)

END()

RECURSE_FOR_TESTS(
    ut
)
