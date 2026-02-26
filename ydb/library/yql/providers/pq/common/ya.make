LIBRARY()

SRCS(
    pq_meta_fields.cpp
    pq_partition_key.cpp
    yql_names.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/public/sdk/cpp/src/client/topic
    yql/essentials/public/types
)

YQL_LAST_ABI_VERSION()

END()
