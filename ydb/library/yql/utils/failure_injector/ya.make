LIBRARY() 
 
OWNER(g:yql)
 
SRCS( 
    failure_injector.cpp 
) 
 
PEERDIR( 
    ydb/library/yql/utils
    ydb/library/yql/utils/log
) 
 
END() 

RECURSE_FOR_TESTS(
    ut
)
