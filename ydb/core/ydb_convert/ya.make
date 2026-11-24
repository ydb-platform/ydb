YQL_LIBRARY()

SRCS(
    column_families.cpp
    compression.cpp
    kesus_description.cpp
    table_settings.cpp
    table_description.cpp
    table_profiles.cpp
    topic_description.cpp
    replication_description.cpp
    external_data_source_description.cpp
    external_table_description.cpp
    ydb_convert.cpp
    tx_proxy_status.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/engine
    ydb/core/external_sources
    ydb/core/formats/arrow/switch
    ydb/core/local_indexes/bloom
    ydb/core/persqueue/events
    ydb/core/protos
    ydb/core/scheme
    ydb/core/util
    ydb/library/conclusion
    ydb/library/mkql_proto/protos
    ydb/public/api/protos
    ydb/public/lib/ydb_cli/dump/util
    yql/essentials/core
    yql/essentials/minikql/dom
    yql/essentials/providers/result/expr_nodes
    yql/essentials/public/udf
    yql/essentials/types/binary_json
    yql/essentials/types/dynumber
)

GENERATE_ENUM_SERIALIZATION(table_description.h)

END()

RECURSE_FOR_TESTS(
    ut
)
