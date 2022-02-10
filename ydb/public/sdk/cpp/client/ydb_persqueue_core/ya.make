LIBRARY()

OWNER(
    g:kikimr
    g:logbroker
)

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h)

SRCS(
    persqueue.h
    proto_accessor.cpp 
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_persqueue_core/impl
    ydb/public/sdk/cpp/client/ydb_proto
)

END()

RECURSE_FOR_TESTS(
    ut
)
