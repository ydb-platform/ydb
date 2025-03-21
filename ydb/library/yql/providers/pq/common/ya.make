LIBRARY()

SRCS(
    pq_meta_fields.cpp
    yql_names.cpp
    pq_partition_key.cpp
)

PEERDIR(
    yql/essentials/public/types
)

YQL_LAST_ABI_VERSION()

END()
