LIBRARY()

SRCS(
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow
    ydb/core/protos
    ydb/core/tx/columnshard/engines/predicate
    ydb/core/tx/columnshard/engines/reader/abstract
    ydb/core/tx/columnshard/engines/reader/actor
    ydb/core/tx/columnshard/engines/reader/common
    ydb/core/tx/columnshard/engines/reader/common_reader
    ydb/core/tx/columnshard/engines/reader/plain_reader
    ydb/core/tx/columnshard/engines/reader/simple_reader
    ydb/core/tx/columnshard/engines/reader/tracing
    ydb/core/tx/columnshard/engines/reader/transaction
    ydb/core/tx/columnshard/engines/scheme
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/resources
    ydb/core/tx/program
)

END()
