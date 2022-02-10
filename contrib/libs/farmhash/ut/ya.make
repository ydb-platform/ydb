UNITTEST_FOR(contrib/libs/farmhash)

OWNER( 
    somov 
) 

NO_COMPILER_WARNINGS()

CFLAGS(-DFARMHASHSELFTEST)

SRCS(test.cc)

END()
