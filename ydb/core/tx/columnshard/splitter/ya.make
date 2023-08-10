LIBRARY()

SRCS(
    splitter.cpp
    batch_slice.cpp
    chunks.cpp
    simple.cpp
    rb_splitter.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/tx/columnshard/engines/storage
    ydb/core/tx/columnshard/engines/scheme
)

END()

RECURSE_FOR_TESTS(
    ut
)
