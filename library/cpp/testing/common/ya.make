LIBRARY()

OWNER(
    amatanhead
    bulatman
    thegeorg
    g:cpp-contrib
)

SRCS(
    env.cpp
    network.cpp
    probe.cpp
    scope.cpp
)

PEERDIR( 
    library/cpp/json 
) 
 
END()

RECURSE_FOR_TESTS(ut)
