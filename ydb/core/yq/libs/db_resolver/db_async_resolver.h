#pragma once
#include <library/cpp/threading/future/future.h>
#include <ydb/core/yq/libs/events/events.h>


namespace NYq {

struct TResolveParams {
    THashMap<std::pair<TString, DatabaseType>, TEvents::TDatabaseAuth> Ids;
    TString TraceId;
};

class IDatabaseAsyncResolver {
public:
    virtual NThreading::TFuture<TEvents::TDbResolverResponse> ResolveIds(const TResolveParams& params) const = 0;

    virtual ~IDatabaseAsyncResolver() = default;
};

} // NYq
