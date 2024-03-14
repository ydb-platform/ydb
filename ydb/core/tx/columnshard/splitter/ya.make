LIBRARY()

SRCS(
    batch_slice.cpp
    chunks.cpp
    simple.cpp
    rb_splitter.cpp
    stats.cpp
    column_info.cpp
    settings.cpp
    scheme_info.cpp
    blob_info.cpp
    chunk_meta.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/tx/columnshard/splitter/abstract
    ydb/core/tx/columnshard/engines/scheme
)

END()

RECURSE_FOR_TESTS(
    ut
)
