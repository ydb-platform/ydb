LIBRARY()

OWNER(
    xenoxeno
    g:kikimr
)

SRCS(
    log.h
    service_impl.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/graph/api
)

END()
