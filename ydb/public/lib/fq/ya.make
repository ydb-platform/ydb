LIBRARY()

SRCS(
    fq.cpp
    scope.cpp
)

PEERDIR(
    library/cpp/json
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/src/client/table
)

END()
