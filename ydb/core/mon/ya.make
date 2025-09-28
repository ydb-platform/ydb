RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    auth.cpp
    auth.h
    events_internal.h
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
    ydb/core/mon/audit
    ydb/core/protos
    ydb/library/aclib
    ydb/library/actors/core
    ydb/library/actors/http
    yql/essentials/public/issue
    ydb/public/sdk/cpp/adapters/issue
    ydb/public/sdk/cpp/src/client/types/status
)

END()

RECURSE(
    audit
    ut_utils
)

RECURSE_FOR_TESTS(
    ut
)
