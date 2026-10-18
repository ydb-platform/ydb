RECURSE_FOR_TESTS(
)

YQL_LIBRARY()

SRCS(
    build_index__cancel.cpp  # gn: into ydb/core/tx/schemeshard
    build_index.cpp  # gn: into ydb/core/tx/schemeshard
    build_index__create.cpp  # gn: into ydb/core/tx/schemeshard
    build_index__forget.cpp  # gn: into ydb/core/tx/schemeshard
    build_index__get.cpp  # gn: into ydb/core/tx/schemeshard
    build_index.h
    build_index_helpers.cpp  # gn: into ydb/core/tx/schemeshard
    build_index_helpers.h
    build_index__list.cpp  # gn: into ydb/core/tx/schemeshard
    build_index__progress.cpp  # gn: into ydb/core/tx/schemeshard
    build_index_tx_base.cpp  # gn: into ydb/core/tx/schemeshard
    build_index_tx_base.h
    common.h
    common.cpp  # gn: into ydb/core/tx/schemeshard
    index_build_info.cpp  # gn: into ydb/core/tx/schemeshard
    index_build_info.h
    index_utils.cpp  # gn: into ydb/core/tx/schemeshard
    index_utils.h
    operation_alter_index.cpp  # gn: into ydb/core/tx/schemeshard
    operation_apply_build_index.cpp  # gn: into ydb/core/tx/schemeshard
    operation_create_build_index.cpp  # gn: into ydb/core/tx/schemeshard
    operation_create_index.cpp  # gn: into ydb/core/tx/schemeshard
    operation_create_indexed_table.cpp  # gn: into ydb/core/tx/schemeshard
    operation_drop_index.cpp  # gn: into ydb/core/tx/schemeshard
    operation_drop_indexed_table.cpp  # gn: into ydb/core/tx/schemeshard
    operation_finalize_build_index.cpp  # gn: into ydb/core/tx/schemeshard
    operation_initiate_build_index.cpp  # gn: into ydb/core/tx/schemeshard
    operation_move_index.cpp  # gn: into ydb/core/tx/schemeshard
    operation_move_table_index.cpp  # gn: into ydb/core/tx/schemeshard
    operation_prepare_index_validation.cpp  # gn: into ydb/core/tx/schemeshard
)

GENERATE_ENUM_SERIALIZATION(index_build_info.h)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/deprecated/enum_codegen
    ydb/core/base
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tablet_flat
    ydb/core/tx
    ydb/core/tx/datashard
)

END()
