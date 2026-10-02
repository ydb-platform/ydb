IF (NOT OPENSOURCE)

LIBRARY()

SRCS(
    yql_qstorage_ydb.cpp
)

PEERDIR(
    yql/essentials/core/qplayer/storage/interface
    yql/essentials/core/qplayer/storage/memory
    contrib/libs/ydb-cpp-sdk/src/client/table
    library/cpp/digest/old_crc
)

END()

RECURSE_FOR_TESTS(
    ut
)

ENDIF()

