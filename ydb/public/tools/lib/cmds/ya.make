PY23_LIBRARY() 
OWNER(g:kikimr) 
 
PY_SRCS( 
    __init__.py 
) 
 
PEERDIR( 
    ydb/tests/library 
    library/python/testing/recipe 
) 
 
END() 
