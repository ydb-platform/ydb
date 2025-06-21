#pragma once
#include <ydb/library/signals/owner.h>

namespace NKikimr::NGeneralCache::NPrivate {

class TCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;

public:
    TCounters(const TString& cacheName, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& baseCounters)
        : TBase("general_cache", baseCounters) {
        DeepSubGroup("cache_name", cacheName);
    }
};

}   // namespace NKikimr::NGeneralCache::NPrivate
