IF (NOT OPENSOURCE)

UNITTEST_FOR(yql/essentials/core/qplayer/storage/ydb)

SRCS(
    yql_qstorage_ydb_ut.cpp
)

PEERDIR(
    yql/essentials/core/qplayer/storage/ut_common
)

END()

ENDIF()

