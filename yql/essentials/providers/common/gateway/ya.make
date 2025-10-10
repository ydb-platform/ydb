LIBRARY()

SRCS(
    yql_provider_gateway.h
    yql_provider_gateway.cpp
)

PEERDIR(
    library/cpp/threading/future
    yql/essentials/ast
)

END()
