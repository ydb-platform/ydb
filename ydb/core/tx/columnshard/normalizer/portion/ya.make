LIBRARY()

SRCS(
    normalizer.cpp
    GLOBAL broken_blobs.cpp
    GLOBAL chunks.cpp
    GLOBAL chunks_actualization.cpp
    GLOBAL chunks_v0_meta.cpp
    GLOBAL clean.cpp
    GLOBAL clean_deprecated_snapshot.cpp
    GLOBAL clean_empty.cpp
    GLOBAL clean_index_columns.cpp
    GLOBAL clean_inserted_portions.cpp
    GLOBAL clean_sub_columns_portions.cpp
    GLOBAL clean_ttl_preset_setting_info.cpp
    GLOBAL clean_ttl_preset_setting_version_info.cpp
    GLOBAL clean_unused_tables_template.cpp
    GLOBAL copy_blob_ids_to_v2.cpp
    GLOBAL leaked_blobs.cpp
    GLOBAL portion.cpp
    GLOBAL restore_appearance_snapshot.cpp
    GLOBAL restore_v1_chunks.cpp
    GLOBAL restore_v2_chunks.cpp
    GLOBAL special_cleaner.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/normalizer/abstract
    ydb/core/tx/columnshard/blobs_reader
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/conveyor/usage
)

END()
