LIBRARY()

SRCS(
    async_http_mon.cpp
    async_http_mon.h
    mon.cpp
    mon.h
    sync_http_mon.cpp
    sync_http_mon.h
    crossref.cpp
    crossref.h
)

PEERDIR(
    library/cpp/json
    library/cpp/lwtrace/mon
    library/cpp/string_utils/url
    ydb/core/base
    ydb/core/grpc_services/base
    ydb/core/protos
    ydb/library/aclib
    ydb/library/actors/core
    ydb/library/actors/http
)

END()
