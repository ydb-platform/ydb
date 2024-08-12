LIBRARY()

SRCS(
    yql_qstorage_ydb.cpp
)

PEERDIR(
    ydb/library/yql/core/qplayer/storage/interface
    ydb/library/yql/core/qplayer/storage/memory
    ydb/public/sdk/cpp/client/ydb_table
    library/cpp/digest/old_crc
)

END()

RECURSE_FOR_TESTS(
    ut
)
