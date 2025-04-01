UNITTEST_FOR(ydb/library/backup)

SIZE(SMALL)

SRC(ut.cpp)

PEERDIR(
    library/cpp/string_utils/quote
    ydb/core/testlib/basics/default
    ydb/library/backup
)

END()
