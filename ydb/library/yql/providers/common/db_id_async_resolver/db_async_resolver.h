#pragma once
#include <library/cpp/threading/future/future.h>
#include <ydb/core/yq/libs/events/events.h>


namespace NYq {

class IDatabaseAsyncResolver {
public:
    virtual NThreading::TFuture<TEvents::TDbResolverResponse> ResolveIds(
        const THashMap<std::pair<TString, DatabaseType>, TEvents::TDatabaseAuth>& ids) const = 0;

    virtual ~IDatabaseAsyncResolver() = default;
};

} // NYq
