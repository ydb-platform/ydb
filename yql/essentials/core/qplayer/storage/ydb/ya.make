IF (NOT OPENSOURCE)

LIBRARY()

SRCS(
    yql_qstorage_ydb.cpp
)

PEERDIR(
    yql/essentials/core/qplayer/storage/interface
    yql/essentials/core/qplayer/storage/memory
    contrib/ydb/public/sdk/cpp/client/ydb_table
    library/cpp/digest/old_crc
)

END()

RECURSE_FOR_TESTS(
    ut
)

ENDIF()

