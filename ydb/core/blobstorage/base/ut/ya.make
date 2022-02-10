UNITTEST_FOR(ydb/core/blobstorage/base)

OWNER(g:kikimr)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/erasure
    ydb/core/protos
)

SRCS(
    batched_vec_ut.cpp 
    bufferwithgaps_ut.cpp
    ptr_ut.cpp
)

END()
