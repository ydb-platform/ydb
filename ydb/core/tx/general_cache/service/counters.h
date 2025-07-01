#pragma once
#include <ydb/core/tx/general_cache/usage/config.h>

#include <ydb/library/accessor/positive_integer.h>
#include <ydb/library/signals/owner.h>

namespace NKikimr::NGeneralCache::NPrivate {

class TManagerCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    const NPublic::TConfig Config;
    const NMonitoring::THistogramPtr RequestDuration;
    const std::shared_ptr<TPositiveControlInteger> TotalInFlight = std::make_shared<TPositiveControlInteger>();
    const std::shared_ptr<TPositiveControlInteger> QueueObjectsCount = std::make_shared<TPositiveControlInteger>();

public:
    bool CheckTotalLimit() const {
        return GetTotalInFlight()->Val() < Config.GetDirectInflightGlobalLimit();
    }

    const std::shared_ptr<TPositiveControlInteger>& GetTotalInFlight() const {
        return TotalInFlight;
    }

    const std::shared_ptr<TPositiveControlInteger>& GetQueueObjectsCount() const {
        return QueueObjectsCount;
    }

    const NPublic::TConfig& GetConfig() const {
        return Config;
    }

    void OnRequestFinished(const TDuration d) const {
        RequestDuration->Collect(d.MicroSeconds());
    }

    const NMonitoring::TDynamicCounters::TCounterPtr RequestCacheMiss;
    const NMonitoring::TDynamicCounters::TCounterPtr RequestCacheHit;
    const NMonitoring::TDynamicCounters::TCounterPtr ObjectCacheMiss;
    const NMonitoring::TDynamicCounters::TCounterPtr ObjectCacheHit;
    const NMonitoring::TDynamicCounters::TCounterPtr CacheSizeCount;
    const NMonitoring::TDynamicCounters::TCounterPtr CacheSizeBytes;
    const NMonitoring::TDynamicCounters::TCounterPtr ObjectsInFlight;
    const NMonitoring::TDynamicCounters::TCounterPtr RequestsInFlight;
    const NMonitoring::TDynamicCounters::TCounterPtr RequestsQueueSize;
    const NMonitoring::TDynamicCounters::TCounterPtr ObjectsQueueSize;
    const NMonitoring::TDynamicCounters::TCounterPtr IncomingRequestsCount;
    const NMonitoring::TDynamicCounters::TCounterPtr IncomingAbortedRequestsCount;
    const NMonitoring::TDynamicCounters::TCounterPtr DirectObjects;
    const NMonitoring::TDynamicCounters::TCounterPtr DirectRequests;
    const NMonitoring::TDynamicCounters::TCounterPtr AbortedRequests;
    const NMonitoring::TDynamicCounters::TCounterPtr AdditionalObjectInfo;
    const NMonitoring::TDynamicCounters::TCounterPtr RemovedObjectInfo;
    const NMonitoring::TDynamicCounters::TCounterPtr FetchedObject;
    const NMonitoring::TDynamicCounters::TCounterPtr NoExistsObject;
    const NMonitoring::TDynamicCounters::TCounterPtr FailedObject;

    TManagerCounters(NColumnShard::TCommonCountersOwner& base, const NPublic::TConfig& config)
        : TBase(base, "signals_owner", "manager")
        , Config(config)
        , RequestDuration(TBase::GetHistogram("Requests/Duration/Us", NMonitoring::ExponentialHistogram(15, 2, 16)))
        , RequestCacheMiss(TBase::GetDeriviative("Cache/Request/Miss/Count"))
        , RequestCacheHit(TBase::GetDeriviative("Cache/Request/Hit/Count"))
        , ObjectCacheMiss(TBase::GetDeriviative("Cache/Object/Miss/Count"))
        , ObjectCacheHit(TBase::GetDeriviative("Cache/Object/Hit/Count"))
        , CacheSizeCount(TBase::GetValue("Cache/Size/Count"))
        , CacheSizeBytes(TBase::GetValue("Cache/Size/Bytes"))
        , ObjectsInFlight(TBase::GetValue("DirectRequest/ObjectsInFlight/Count"))
        , RequestsInFlight(TBase::GetValue("DirectRequest/RequestsInFlight/Count"))
        , RequestsQueueSize(TBase::GetValue("RequestsQueue/Size/Count"))
        , ObjectsQueueSize(TBase::GetValue("ObjectsQueue/Size/Count"))
        , IncomingRequestsCount(TBase::GetDeriviative("Incoming/Requests/Count"))
        , IncomingAbortedRequestsCount(TBase::GetDeriviative("Incoming/AbortedRequests/Count"))
        , DirectObjects(TBase::GetDeriviative("Direct/Object/Count"))
        , DirectRequests(TBase::GetDeriviative("Direct/Request/Count"))
        , AbortedRequests(TBase::GetDeriviative("AbortedRequest/Count"))
        , AdditionalObjectInfo(TBase::GetDeriviative("AdditionalInfo/Count"))
        , RemovedObjectInfo(TBase::GetDeriviative("RemovedInfo/Count"))
        , FetchedObject(TBase::GetDeriviative("DirectRequest/Fetched/Count"))
        , NoExistsObject(TBase::GetDeriviative("DirectRequest/NoExists/Count"))
        , FailedObject(TBase::GetDeriviative("DirectRequest/Failed/Count")) {
    }
};

class TActorCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    YDB_READONLY_DEF(std::shared_ptr<TManagerCounters>, Manager);

public:
    TActorCounters(const TString& cacheName, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& baseCounters, const NPublic::TConfig& config)
        : TBase("general_cache", baseCounters) {
        DeepSubGroup("cache_name", cacheName);
        Manager = std::make_shared<TManagerCounters>(*this, config);
    }
};

}   // namespace NKikimr::NGeneralCache::NPrivate
