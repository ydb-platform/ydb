LIBRARY() 
 
OWNER(g:yql)
 
PEERDIR( 
    ydb/library/yql/utils/backtrace
) 
 
YQL_LAST_ABI_VERSION()
 
SRCS( 
    backtrace.cpp 
    symbolizer.cpp 
) 
 
END() 
