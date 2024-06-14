LIBRARY()

SRCS(
    yql_qstorage_file.cpp
)

PEERDIR(
    ydb/library/yql/core/qplayer/storage/interface
    ydb/library/yql/core/qplayer/storage/memory
    library/cpp/digest/old_crc
)

END()

RECURSE_FOR_TESTS(
    ut
)
