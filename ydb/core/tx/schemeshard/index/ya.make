RECURSE_FOR_TESTS(
)

LIBRARY()

SRCS(
    build_index__cancel.cpp
    build_index.cpp
    build_index__create.cpp
    build_index__forget.cpp
    build_index__get.cpp
    build_index.h
    build_index_helpers.cpp
    build_index_helpers.h
    build_index__list.cpp
    build_index__progress.cpp
    build_index_tx_base.cpp
    build_index_tx_base.h
    common.h
    common.cpp
    index_build_info.cpp
    index_build_info.h
    index_utils.cpp
    index_utils.h
    operation_alter_index.cpp
    operation_apply_build_index.cpp
    operation_create_build_index.cpp
    operation_create_index.cpp
    operation_create_indexed_table.cpp
    operation_drop_index.cpp
    operation_drop_indexed_table.cpp
    operation_finalize_build_index.cpp
    operation_initiate_build_index.cpp
    operation_move_index.cpp
    operation_move_table_index.cpp
    operation_prepare_index_validation.cpp
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

YQL_LAST_ABI_VERSION()

END()
