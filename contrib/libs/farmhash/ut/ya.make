UNITTEST_FOR(contrib/libs/farmhash)

SUBSCRIBER(
    g:cpp-contrib
)

NO_COMPILER_WARNINGS()

CFLAGS(-DFARMHASHSELFTEST)

SRCS(test.cc)

END()
