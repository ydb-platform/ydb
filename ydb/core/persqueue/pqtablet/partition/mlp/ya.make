LIBRARY()

SRCS(
    mlp_common.cpp
    mlp_consumer.cpp
    mlp_message_enricher.cpp
    mlp_storage.cpp
    mlp_storage__serialization.cpp
)



PEERDIR(
    ydb/core/persqueue/events
    ydb/core/persqueue/common
    ydb/core/persqueue/common/proxy
    ydb/core/persqueue/pqtablet/common
    ydb/core/persqueue/public/write_meta
)


GENERATE_ENUM_SERIALIZATION(mlp_storage.h)

END()

RECURSE_FOR_TESTS(
    ut
)
