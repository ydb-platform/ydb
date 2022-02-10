LIBRARY() 
 
OWNER(
    laplab
    g:kikimr
)
 
YQL_ABI_VERSION(
    2
    21
    0
)

PEERDIR( 
    library/cpp/json
    ydb/library/yql/minikql/dom
) 
 
SRCS( 
    format.cpp 
    read.cpp 
    write.cpp 
) 
 
GENERATE_ENUM_SERIALIZATION(format.h) 
 
END()

RECURSE_FOR_TESTS(
    ut
)
