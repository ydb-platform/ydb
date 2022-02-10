LIBRARY()

OWNER(
    spuchin
    g:yql_ydb_core
)

SRCS(
    yql_provider_gateway.h
    yql_provider_gateway.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/library/yql/ast 
)

END()
