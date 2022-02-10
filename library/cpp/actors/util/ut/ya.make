UNITTEST_FOR(library/cpp/actors/util)

IF (WITH_VALGRIND) 
    TIMEOUT(600) 
    SIZE(MEDIUM) 
ENDIF() 
 
OWNER(
    alexvru
    g:kikimr
)

SRCS(
    rope_ut.cpp
    unordered_cache_ut.cpp
)

END()
