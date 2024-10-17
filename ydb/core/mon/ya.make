LIBRARY()

SRCS(
    async_http_mon.cpp
    crossref.cpp
    dynamic_counters_page.cpp
    mon.cpp
    sync_http_mon.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/lwtrace/mon
    library/cpp/protobuf/json
    library/cpp/string_utils/url
    ydb/core/base
    ydb/core/grpc_services/base
    ydb/core/protos
    ydb/library/aclib
    ydb/library/actors/core
    ydb/library/actors/http
    ydb/library/yql/public/issue
    ydb/public/sdk/cpp/client/ydb_types/status
)

END()
