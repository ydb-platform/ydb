LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/services/metadata/abstract
    ydb/services/bg_tasks/abstract
    ydb/services/bg_tasks/protos
    ydb/library/services
)

END()

RECURSE_FOR_TESTS(
    ut
)
