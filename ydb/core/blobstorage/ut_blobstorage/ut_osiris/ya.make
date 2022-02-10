UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)
 
OWNER(g:kikimr)
 
FORK_SUBTESTS()
 
SIZE(LARGE)
 
TIMEOUT(3600)
 
TAG(ya:fat)

SRCS(
    osiris.cpp
)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
)

REQUIREMENTS(
    cpu:4
    ram:32
)

END() 
