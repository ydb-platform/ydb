LIBRARY()

SRCS(
    abstract_scheme.cpp  # gn: into ydb/core/tx/columnshard/engines/portions
    snapshot_scheme.cpp  # gn: into ydb/core/tx/columnshard/engines/portions
    filtered_scheme.cpp  # gn: into ydb/core/tx/columnshard/engines/portions
    versioned_index.cpp  # gn: into ydb/core/tx/columnshard/engines/portions
    preset_schemas.cpp  # gn: into ydb/core/tx/columnshard/engines/portions
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/abstract
    ydb/core/tx/columnshard/engines/scheme/common
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/library/actors/core
)

END()
