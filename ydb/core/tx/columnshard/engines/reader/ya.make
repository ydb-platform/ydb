LIBRARY()

SRCS(
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/predicate
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/resources
    ydb/core/tx/program
    ydb/core/tx/columnshard/engines/reader/plain_reader
    ydb/core/tx/columnshard/engines/reader/sys_view
    ydb/core/tx/columnshard/engines/reader/abstract
    ydb/core/tx/columnshard/engines/reader/common
    ydb/core/tx/columnshard/engines/reader/actor
    ydb/core/tx/columnshard/engines/reader/transaction
    ydb/core/tx/columnshard/engines/scheme
)

END()
