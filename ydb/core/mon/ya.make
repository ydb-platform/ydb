LIBRARY()

SRCS(
    mon.cpp
    mon.h
    crossref.cpp
    crossref.h
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
    ydb/library/actors/http/audit
    yql/essentials/public/issue
    ydb/public/sdk/cpp/adapters/issue
    ydb/public/sdk/cpp/src/client/types/status
)

END()
