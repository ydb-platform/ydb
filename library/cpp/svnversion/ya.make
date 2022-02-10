OWNER(g:cpp-contrib)

LIBRARY()

SRCS(
    svnversion.cpp
    svn_interface.c 
)
END()
RECURSE( 
    test 
) 
