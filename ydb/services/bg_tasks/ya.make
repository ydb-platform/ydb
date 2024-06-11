LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    ydb/services/bg_tasks/abstract
    ydb/services/bg_tasks/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
