RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    access_service.cpp
    access_service.h
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/grpc/actor_client
    ydb/library/ncloud/api
    ydb/core/base
)

END()
