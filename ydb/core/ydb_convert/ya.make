LIBRARY()

SRCS(
    column_families.cpp
    compression.cpp
    table_settings.cpp
    table_description.cpp
    table_profiles.cpp
    topic_description.cpp
    ydb_convert.cpp
    tx_proxy_status.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/engine
    ydb/core/formats/arrow/switch
    yql/essentials/core
    ydb/core/protos
    ydb/core/scheme
    ydb/core/util
    yql/essentials/types/binary_json
    yql/essentials/types/dynumber
    ydb/library/conclusion
    ydb/library/mkql_proto/protos
    yql/essentials/minikql/dom
    yql/essentials/public/udf
    ydb/public/api/protos
    ydb/core/tx/schemeshard/olap/column_family
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
