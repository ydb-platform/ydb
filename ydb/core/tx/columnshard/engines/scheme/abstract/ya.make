LIBRARY()

SRCS(
    index_info.cpp
    column_ids.cpp
    schema_version.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/formats/arrow/save_load
)

YQL_LAST_ABI_VERSION()

END()
