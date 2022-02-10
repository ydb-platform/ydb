#pragma once
#include <library/cpp/threading/future/future.h>
#include <ydb/core/yq/libs/events/events.h> 


namespace NYq {

struct TResolveParams { //TODO: remove
    THashMap<std::pair<TString, DatabaseType>, TEvents::TDatabaseAuth> Ids;
    TString TraceId;
};

class IDatabaseAsyncResolver {
public:
    using TEndpoint = TEvents::TEvEndpointResponse::TEndpoint;

    virtual NThreading::TFuture<THashMap<std::pair<TString, DatabaseType>, TEndpoint>> ResolveIds(const TResolveParams& params) const = 0;

    virtual ~IDatabaseAsyncResolver() = default;
};

} // NYq
