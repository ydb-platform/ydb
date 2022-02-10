LIBRARY() 
 
OWNER(
    g:kikimr
    g:yql
    g:yql_ydb_core
)
 
SRCS( 
    backtrace.cpp 
) 
 
PEERDIR(
    contrib/libs/llvm12/lib/DebugInfo/Symbolize
)

END() 
