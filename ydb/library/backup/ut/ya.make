UNITTEST_FOR(ydb/library/backup)

SIZE(SMALL)

SRC(ut.cpp)

PEERDIR(
    library/cpp/string_utils/quote
    ydb/library/backup
)

END()
