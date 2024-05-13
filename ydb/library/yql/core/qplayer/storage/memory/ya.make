LIBRARY()

SRCS(
    yql_qstorage_memory.cpp
)

PEERDIR(
    ydb/library/yql/core/qplayer/storage/interface
)

END()

RECURSE_FOR_TESTS(
    ut
)
