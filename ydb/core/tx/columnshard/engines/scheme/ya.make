LIBRARY()

SRCS(
    abstract_scheme.cpp
    snapshot_scheme.cpp
    filtered_scheme.cpp
    index_info.cpp
    tier_info.cpp
    column_features.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/formats/arrow

    ydb/library/actors/core
    ydb/core/tx/columnshard/engines/scheme/indexes
    ydb/core/tx/columnshard/engines/scheme/abstract
    ydb/core/tx/columnshard/engines/scheme/versions
    ydb/core/tx/columnshard/engines/scheme/tiering
    ydb/core/tx/columnshard/engines/scheme/column
    ydb/core/tx/columnshard/engines/scheme/defaults
    ydb/core/tx/columnshard/blobs_action/abstract
)

YQL_LAST_ABI_VERSION()

END()
