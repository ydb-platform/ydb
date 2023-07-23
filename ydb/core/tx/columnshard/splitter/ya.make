LIBRARY()

SRCS(
    splitter.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/tx/columnshard/engines/storage
    ydb/core/tx/columnshard/engines/scheme
)

END()
