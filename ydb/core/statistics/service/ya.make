LIBRARY()

SRCS(
    http_request.h
    http_request.cpp
    service.h
    service_impl.cpp 
)

PEERDIR(
    library/cpp/json
    ydb/core/base
    ydb/core/engine/minikql
    ydb/core/protos
    ydb/core/statistics
    ydb/core/statistics/database
    ydb/core/tablet
    ydb/core/tablet_flat
    yql/essentials/core/histogram
    yql/essentials/core/minsketch
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)

