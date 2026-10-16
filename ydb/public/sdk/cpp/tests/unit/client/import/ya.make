UNITTEST()

SIZE(MEDIUM)
FORK_SUBTESTS()

PEERDIR(
    library/cpp/testing/unittest
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/import
)

SRCS(import_ut.cpp)

END()
