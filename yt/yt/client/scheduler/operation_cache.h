#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/scheduler/operation_id_or_alias.h>

#include <yt/yt/core/misc/async_expiring_cache.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

//! This cache is able to store cached results `GetOperation` for given set of attributes.
class TOperationCache
    : public TAsyncExpiringCache<TOperationIdOrAlias, NYson::TYsonString>
{
public:
    TOperationCache(
        TAsyncExpiringCacheConfigPtr config,
        THashSet<TString> attributes,
        NApi::IClientPtr client,
        NProfiling::TProfiler profiler = {});

private:
    const THashSet<TString> Attributes_;
    const NApi::IClientPtr Client_;

    TFuture<NYson::TYsonString> DoGet(
        const TOperationIdOrAlias& key,
        bool isPeriodicUpdate) noexcept override;
};

DEFINE_REFCOUNTED_TYPE(TOperationCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
