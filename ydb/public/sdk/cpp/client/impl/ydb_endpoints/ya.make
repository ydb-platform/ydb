LIBRARY()

OWNER(
    dcherednik
    g:kikimr
)

SRCS(
    endpoints.cpp
)

PEERDIR(
    library/cpp/monlib/metrics
    ydb/public/api/grpc
)

END()

RECURSE_FOR_TESTS(
    ut
)
