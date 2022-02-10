LIBRARY()
 
OWNER( 
    alexvru 
    g:kikimr 
) 
 
PEERDIR( 
    ydb/core/base 
    ydb/core/blobstorage 
    ydb/core/blobstorage/vdisk/common 
) 
 
SRCS( 
    defs.h 
    dsproxy_mock.cpp 
    dsproxy_mock.h 
) 
 
END()
