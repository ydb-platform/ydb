UNITTEST_FOR(ydb/library/backup)

SIZE(SMALL)
REQUIREMENTS(cpu:1)

SRC(ut.cpp)

PEERDIR(
    library/cpp/string_utils/quote
    ydb/library/backup
)

END()
