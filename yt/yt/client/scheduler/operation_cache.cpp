#include "operation_cache.h"
#include "private.h"

#include <yt/yt/client/api/client.h>

namespace NYT::NScheduler {

using namespace NApi;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TOperationCache::TOperationCache(
    TAsyncExpiringCacheConfigPtr config,
    THashSet<TString> attributes,
    NApi::IClientPtr client,
    NProfiling::TProfiler profiler)
    : TAsyncExpiringCache(
        std::move(config),
        SchedulerLogger().WithTag("Cache: Operation"),
        std::move(profiler))
    , Attributes_(std::move(attributes))
    , Client_(std::move(client))
{ }

TFuture<TYsonString> TOperationCache::DoGet(const TOperationIdOrAlias& key, bool /*isPeriodicUpdate*/) noexcept
{
    auto options = TGetOperationOptions {
        .Attributes = Attributes_,
        .IncludeRuntime = true
    };

    return Client_->GetOperation(key, options).Apply(BIND([] (const TOperation& operation) {
        return NYson::ConvertToYsonString(operation);
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
