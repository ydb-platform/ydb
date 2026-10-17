LIBRARY()

SRCS(
    batch_slice.cpp
    chunks.cpp
    column_info.cpp
    settings.cpp  # gn: settings
    blob_info.cpp
    chunk_meta.cpp
)
# gn: settings headers settings.h

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/tx/columnshard/splitter/abstract
    ydb/core/tx/columnshard/engines/scheme
    ydb/core/formats/arrow/splitter
)

END()

RECURSE_FOR_TESTS(
    ut
)
