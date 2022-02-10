UNITTEST()

FORK_SUBTESTS() 
 
OWNER(
    alexvru
    g:kikimr
)

REQUIREMENTS( 
    cpu:4 
    ram:16 
) 
 
IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE) 
    TAG(ya:fat) 
    TIMEOUT(1800) 
ELSE() 
    SIZE(MEDIUM) 
    TIMEOUT(600) 
ENDIF() 
 
SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage
    ydb/core/blobstorage/base
    ydb/core/blobstorage/dsproxy/mock
    ydb/core/mind/bscontroller
    ydb/core/protos
    ydb/core/testlib
    ydb/core/testlib/basics
)

YQL_LAST_ABI_VERSION()

REQUIREMENTS(network:full)

END()
