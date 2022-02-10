LIBRARY() 
OWNER( 
    amatanhead
    bulatman 
    dancingqueue 
    prettyboy
    thegeorg
    g:cpp-contrib 
) 
 
PEERDIR( 
    contrib/restricted/googletest/googlemock
    contrib/restricted/googletest/googletest
) 
 
SRCS( 
    assertions.cpp
    gtest_extensions.cpp
    matchers.cpp
    pretty_printers.cpp
    probe.cpp
) 
 
END() 
 
RECURSE_FOR_TESTS(ut) 
