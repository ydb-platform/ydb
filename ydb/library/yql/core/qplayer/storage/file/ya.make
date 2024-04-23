LIBRARY()

SRCS(
    yql_qstorage_file.cpp
)

PEERDIR(
    ydb/library/yql/core/qplayer/storage/interface
    ydb/library/yql/core/qplayer/storage/memory
)

END()

RECURSE_FOR_TESTS(
    ut
)
