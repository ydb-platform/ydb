UNITTEST_FOR(library/cpp/digest/md5)

SIZE(MEDIUM)

TIMEOUT(120)

OWNER(
    pg
    g:util
)

SRCS(
    md5_medium_ut.cpp
)

REQUIREMENTS(ram:10)

END()
