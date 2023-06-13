UNITTEST_FOR(ydb/library/backup)

SIZE(SMALL)

TIMEOUT(60)

SRC(ut.cpp)

PEERDIR(
    library/cpp/string_utils/quote
    ydb/library/backup
)

END()
