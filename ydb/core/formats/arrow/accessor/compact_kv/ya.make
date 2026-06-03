LIBRARY()

PEERDIR(
    contrib/libs/apache/avro
    ydb/core/formats/arrow/accessor/abstract
    ydb/core/formats/arrow/accessor/plain
    ydb/core/formats/arrow/accessor/common
    ydb/core/formats/arrow/serializer
    ydb/core/formats/arrow/common
    ydb/library/formats/arrow
    ydb/library/formats/arrow/protos
    library/cpp/json
    library/cpp/streams/zstd
    yql/essentials/types/binary_json
)

ADDINCL(
    contrib/libs/apache/avro/include
)

NO_COMPILER_WARNINGS()

SRCS(
    GLOBAL constructor.cpp
    GLOBAL request.cpp
    accessor.cpp
    serializer.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    builder
    reader
    checker
)
