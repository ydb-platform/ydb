#pragma once

#include "defs.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/group_stat.h>
#include <ydb/core/blobstorage/base/common_latency_hist_bounds.h>
#include <ydb/core/mon/mon.h>

#include <atomic>

#include <util/digest/numeric.h>
#include <util/generic/bitops.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/system/mutex.h>
#include <ydb/core/util/max_tracker.h>

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Storage pool monitoring counters
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TRequestMonItem {
    ::NMonitoring::TDynamicCounters::TCounterPtr RequestBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr GeneratedSubrequests;
    ::NMonitoring::TDynamicCounters::TCounterPtr GeneratedSubrequestBytes;
    NMonitoring::THistogramPtr ResponseTime;
    TMaxTracker ResponseTimeMax;
    TMaxTracker InFlightResponseTimeMax;
    ::NMonitoring::TDynamicCounters::TCounterPtr ResponseTimeUsCompletedSum;
    ::NMonitoring::TDynamicCounters::TCounterPtr ResponseTimeCompletedCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlightResponseTimeUsSum;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlightCount;

private:
    static constexpr ui64 InFlightRequestsShardCount = 64;
    static_assert((InFlightRequestsShardCount & (InFlightRequestsShardCount - 1)) == 0);

    struct alignas(64) TInFlightRequestsShard {
        std::atomic<ui64> RequestCount = 0;
        std::atomic<ui64> RequestStartTimeUsSum = 0;
        TMutex Mutex;
        THashMap<ui64, TMonotonic> Requests;
    };

    TInFlightRequestsShard InFlightRequestsShards[InFlightRequestsShardCount];

    TInFlightRequestsShard& GetInFlightRequestsShard(ui64 requestId) {
        return InFlightRequestsShards[IntHash(requestId) & (InFlightRequestsShardCount - 1)];
    }

public:

    void Init(TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, NPDisk::EDeviceType type) {
        RequestBytes = counters->GetCounter("requestBytes", true);
        GeneratedSubrequests = counters->GetCounter("generatedSubrequests", true);
        GeneratedSubrequestBytes = counters->GetCounter("generatedSubrequestBytes", true);

        NMonitoring::TBucketBounds bounds = GetCommonLatencyHistBounds(type);

        ResponseTime = counters->GetHistogram("responseTimeMs",
            NMonitoring::ExplicitHistogram(std::move(bounds)));
        ResponseTimeMax.Init(counters->GetCounter("responseTimeMsMax", false));
        InFlightResponseTimeMax.Init(counters->GetCounter("inFlightResponseTimeMsMax", false));
        ResponseTimeUsCompletedSum = counters->GetCounter("responseTimeUsCompletedSum", true);
        ResponseTimeCompletedCount = counters->GetCounter("responseTimeCompletedCount", true);
        InFlightResponseTimeUsSum = counters->GetCounter("inFlightResponseTimeUsSum", false);
        InFlightCount = counters->GetCounter("inFlightCount", false);
    }

    void Register(ui32 requestBytes, ui32 generatedSubrequests, ui32 generatedSubrequestBytes, double durationSeconds) {
        const double durationMs = durationSeconds * 1000.0;
        const ui64 durationMsInt = durationMs;
        const ui64 durationUsInt = durationSeconds * 1'000'000.0;
        *RequestBytes += requestBytes;
        *GeneratedSubrequests += generatedSubrequests;
        *GeneratedSubrequestBytes += generatedSubrequestBytes;
        ResponseTime->Collect(durationMs);
        ResponseTimeMax.Collect(durationMsInt);
        ResponseTimeUsCompletedSum->Add(durationUsInt);
        ResponseTimeCompletedCount->Inc();
    }

    void AddInFlightRequest(ui64 requestId, TMonotonic receivedTime) {
        TInFlightRequestsShard& shard = GetInFlightRequestsShard(requestId);
        TGuard<TMutex> guard(shard.Mutex);
        const auto [it, inserted] = shard.Requests.emplace(requestId, receivedTime);
        Y_UNUSED(it);
        if (inserted) {
            shard.RequestCount.fetch_add(1, std::memory_order_relaxed);
            shard.RequestStartTimeUsSum.fetch_add(receivedTime.MicroSeconds(), std::memory_order_relaxed);
        }
    }

    void RemoveInFlightRequest(ui64 requestId) {
        TInFlightRequestsShard& shard = GetInFlightRequestsShard(requestId);
        TGuard<TMutex> guard(shard.Mutex);
        auto it = shard.Requests.find(requestId);
        if (it != shard.Requests.end()) {
            shard.RequestCount.fetch_sub(1, std::memory_order_relaxed);
            shard.RequestStartTimeUsSum.fetch_sub(it->second.MicroSeconds(), std::memory_order_relaxed);
            shard.Requests.erase(it);
        }
    }

    void Update() {
        Update(TMonotonic::Now());
    }

    void Update(TMonotonic now) {
        ui64 inFlightCount = 0;
        ui64 startTimeUsSum = 0;
        for (TInFlightRequestsShard& shard : InFlightRequestsShards) {
            inFlightCount += shard.RequestCount.load(std::memory_order_relaxed);
            startTimeUsSum += shard.RequestStartTimeUsSum.load(std::memory_order_relaxed);
        }

        const ui64 nowUs = now.MicroSeconds();
        const ui64 nowUsSum = inFlightCount * nowUs;
        const ui64 latencyUsSum = nowUsSum >= startTimeUsSum
            ? nowUsSum - startTimeUsSum
            : 0;

        ui64 latencyMsMax = 0;
        for (TInFlightRequestsShard& shard : InFlightRequestsShards) {
            TGuard<TMutex> guard(shard.Mutex);
            for (const auto& [requestId, receivedTime] : shard.Requests) {
                Y_UNUSED(requestId);
                if (now > receivedTime) {
                    const TDuration latency = now - receivedTime;
                    latencyMsMax = Max<ui64>(latencyMsMax, latency.MilliSeconds());
                }
            }
        }

        InFlightResponseTimeUsSum->Set(latencyUsSum);
        InFlightCount->Set(inFlightCount);
        InFlightResponseTimeMax.Collect(latencyMsMax);
        InFlightResponseTimeMax.Update();
        ResponseTimeMax.Update();
    }
};

class TStoragePoolCounters : public TThrRefBase {
public:
    enum EHandleClass {
        HcPutTabletLog = 0,
        HcPutUserData = 1,
        HcPutAsync = 2,
        HcGetFast = 3,
        HcGetAsync = 4,
        HcGetDiscover = 5,
        HcGetLow = 6,
        HcCount = 7
    };

private:
    static TString GetHandleClassName(EHandleClass handleClass) {
        switch (handleClass) {
            case HcPutTabletLog:
                return "PutTabletLog";
            case HcPutUserData:
                return "PutUserData";
            case HcPutAsync:
                return "PutAsync";
            case HcGetFast:
                return "GetFast";
            case HcGetAsync:
                return "GetAsync";
            case HcGetDiscover:
                return "GetDiscover";
            case HcGetLow:
                return "GetLow";
            case HcCount:
                return "Unknown";
        }
        return "Unknown";
    }

    static bool IsReducedHandleClass(EHandleClass handleClass) {
        return (handleClass == HcPutAsync
            || handleClass == HcGetAsync
            || handleClass == HcGetDiscover
            || handleClass == HcGetLow);
    }

    // common size classes

    // Old buckets are: 64 128 256 512 1k 2k 4k 8k 16k 32k 64k 128k 256k 512k 1M 2M 4M 8M 16M -- 19 buckets
    // Buckets are: 256 4k 256k 1M 4M 16M -- 6 buckets
    static constexpr ui32 MaxSizeClassBucketIdx = 5;
    static constexpr const char *const SizeClassNameList[MaxSizeClassBucketIdx + 1] =
      {"256", "4096", "262144", "1048576", "4194304", "16777216"};

public:
    static ui32 SizeClassFromSizeBytes(ui32 requestBytes) {
        if (requestBytes <= 4*1024) {
            if (requestBytes <= 256) {
                return 0;
            } else {
                return 1;
            }
        } else {
            if (requestBytes <= 1*1024*1024) {
                if (requestBytes <= 256*1024) {
                    return 2;
                } else {
                    return 3;
                }
            } else {
                if (requestBytes < 4*1024*1024) {
                    return 4;
                } else {
                    return 5;
                }
            }
        }
    }

private:
    static const char* SizeClassName(ui32 sizeClass) {
        return SizeClassNameList[Min<ui32>(MaxSizeClassBucketIdx, sizeClass)];
    }

    // reduced size classes for PutAsync, GetAsync, GetLow, GetDiscover

    static constexpr ui32 MaxReducedSizeClassBucketIdx = 2;
    static constexpr const char *const ReducedSizeClassNameList[MaxReducedSizeClassBucketIdx + 1] =
      {"262144", "1048576", "16777216"};

public:
    static ui32 ReducedSizeClassFromSizeBytes(ui32 requestBytes) {
        if (requestBytes <= 256*1024) {
            return 0;
        } else if (requestBytes <= 1*1024*1024) {
            return 1;
        } else {
            return 2;
        }
    }

private:
    static const char* ReducedSizeClassName(ui32 sizeClass) {
        return ReducedSizeClassNameList[Min<ui32>(MaxReducedSizeClassBucketIdx, sizeClass)];
    }

    static_assert(MaxReducedSizeClassBucketIdx < MaxSizeClassBucketIdx);

    TRequestMonItem RequestMon[HcCount][MaxSizeClassBucketIdx + 1];
    TString StoragePoolName;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> PoolGroup;

public:
    TRequestMonItem& GetItem(EHandleClass handleClass, ui32 requestBytes) {
        Y_ABORT_UNLESS((ui32)handleClass < (ui32)HcCount);
        ui32 sizeClassIdx = 0;
        if (IsReducedHandleClass(handleClass)) {
            sizeClassIdx = ReducedSizeClassFromSizeBytes(requestBytes);
            Y_ABORT_UNLESS(sizeClassIdx <= MaxReducedSizeClassBucketIdx);
        } else {
            sizeClassIdx = SizeClassFromSizeBytes(requestBytes);
            Y_ABORT_UNLESS(sizeClassIdx <= MaxSizeClassBucketIdx);
        }
        return RequestMon[(ui32)handleClass][sizeClassIdx];
    }

    TStoragePoolCounters(TIntrusivePtr<::NMonitoring::TDynamicCounters> &counters, const TString &storagePoolName,
            NPDisk::EDeviceType type)
        : StoragePoolName(storagePoolName)
        , PoolGroup(counters->GetSubgroup("storagePool", storagePoolName))
    {
        for (ui32 handleClass = 0; handleClass < (ui32)HcCount; ++handleClass) {
            TString handleClassName = GetHandleClassName((EHandleClass)handleClass);
            TIntrusivePtr<::NMonitoring::TDynamicCounters> hcGroup = PoolGroup->GetSubgroup("handleClass", handleClassName);
            if (IsReducedHandleClass((EHandleClass)handleClass)) {
                for (ui32 sizeClassIdx = 0; sizeClassIdx <= MaxReducedSizeClassBucketIdx; ++sizeClassIdx) {
                    TString sizeClassName = ReducedSizeClassName(sizeClassIdx);
                    RequestMon[handleClass][sizeClassIdx].Init(hcGroup->GetSubgroup("sizeClass", sizeClassName), type);
                }
            } else {
                for (ui32 sizeClassIdx = 0; sizeClassIdx <= MaxSizeClassBucketIdx; ++sizeClassIdx) {
                    TString sizeClassName = SizeClassName(sizeClassIdx);
                    RequestMon[handleClass][sizeClassIdx].Init(hcGroup->GetSubgroup("sizeClass", sizeClassName), type);
                }
            }
        }

        // request cost counters
        DSProxyDiskCostCounter = PoolGroup->GetCounter("DSProxyDiskCostNs", true);
    }

    void Update() {
        for (ui32 handleClass = 0; handleClass < (ui32)HcCount; ++handleClass) {
            ui32 maxIdx = IsReducedHandleClass((EHandleClass)handleClass) ? MaxReducedSizeClassBucketIdx
                                                                            : MaxSizeClassBucketIdx;
            for (ui32 sizeClassIdx = 0; sizeClassIdx <= maxIdx; ++sizeClassIdx) {
                RequestMon[handleClass][sizeClassIdx].Update();
            }
        }
    }

public:
    // request cost counters
    ::NMonitoring::TDynamicCounters::TCounterPtr DSProxyDiskCostCounter;
};

class TDsProxyPerPoolCounters : public TThrRefBase {
protected:
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TMap<TString, TIntrusivePtr<TStoragePoolCounters>> StoragePoolCounters;

public:
    TDsProxyPerPoolCounters(TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) {
      TIntrusivePtr<::NMonitoring::TDynamicCounters> group = GetServiceCounters(counters, "dsproxynode");
      Counters = group->GetSubgroup("subsystem", "request");
    };

    TIntrusivePtr<TStoragePoolCounters> GetPoolCounters(const TString &storagePoolName,
            NPDisk::EDeviceType type = NPDisk::DEVICE_TYPE_UNKNOWN) {
        auto it = StoragePoolCounters.find(storagePoolName);
        if (it != StoragePoolCounters.end()) {
            return it->second;
        }
        TIntrusivePtr<TStoragePoolCounters> spc = MakeIntrusive<TStoragePoolCounters>(Counters, storagePoolName, type);
        StoragePoolCounters.emplace(storagePoolName, spc);
        return spc;
    }

    void UpdateAll() {
        for (auto &kv : StoragePoolCounters) {
            kv.second->Update();
        }
    }
};


} // NKikimr
