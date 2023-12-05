LIBRARY()

SRCS(
    abstract.cpp
    GLOBAL hash_by_columns.cpp
    container.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/formats/arrow/hash
)

YQL_LAST_ABI_VERSION()
GENERATE_ENUM_SERIALIZATION(hash_by_columns.h)

END()
