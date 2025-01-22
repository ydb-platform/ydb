LIBRARY()

SRCS(
    log.h
    service_impl.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/graph/api
    ydb/public/sdk/cpp/src/client/params
)

END()
