LIBRARY()

SRCS(codec.cpp)

PEERDIR(
    ydb/public/api/grpc/draft/xds/wire
    ydb/public/api/protos
)

END()

RECURSE_FOR_TESTS(ut)
