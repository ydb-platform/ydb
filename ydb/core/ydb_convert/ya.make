LIBRARY()

SRCS(
    column_families.cpp
    compression.cpp
    table_settings.cpp
    table_description.cpp
    table_profiles.cpp
    ydb_convert.cpp
    tx_proxy_status.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/engine
    ydb/core/formats/arrow/switch
    ydb/library/yql/core
    ydb/core/protos
    ydb/core/scheme
    ydb/core/util
    ydb/library/binary_json
    ydb/library/dynumber
    ydb/library/mkql_proto/protos
    ydb/library/yql/minikql/dom
    ydb/library/yql/public/udf
    ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
