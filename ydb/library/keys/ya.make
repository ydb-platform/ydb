LIBRARY() 
 
OWNER(
    cthulhu
    g:kikimr
)
 
SRCS( 
    default_keys.cpp 
) 
 
END() 

RECURSE_FOR_TESTS(
    ut
)
