UNITTEST_FOR(ydb/core/nbs/cloud/storage/core/libs/grpc)

SRCDIR(ydb/core/nbs/cloud/storage/core/libs/grpc)

SRCS(
    shutdown_ut.cpp
)

ADDINCL(
    contrib/libs/grpc
)

END()
