LIBRARY()

SRCS(
    http_request.h
    http_request.cpp
    service.h
    service.cpp   
    service_impl.cpp 
)

PEERDIR(
    ydb/core/base
    ydb/core/engine/minikql
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/statistics/database    
    ydb/library/minsketch
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)

