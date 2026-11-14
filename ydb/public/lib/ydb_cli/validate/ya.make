LIBRARY()

SRCS(
    validate.cpp
)

PEERDIR(
    contrib/libs/openssl
    contrib/libs/zstd
    library/cpp/json
    ydb/library/backup/proto
    ydb/public/api/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
