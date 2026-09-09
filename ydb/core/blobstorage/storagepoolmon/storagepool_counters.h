#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/group_stat.h>
#include <ydb/core/blobstorage/base/common_latency_hist_bounds.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>

#include <cstdint>

#include <util/generic/bitops.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <ydb/core/util/max_tracker.h>

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Storage pool monitoring counters
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TStoragePoolCounters;

struct TDsProxyInFlightLatencyStats {
    ui64 InFlightLatencyUsSum = 0;
    ui64 InFlightCount = 0;
    ui64 InFlightLatencyUsMax = 0;

    void Add(const TDsProxyInFlightLatencyStats& other) {
        InFlightLatencyUsSum += other.InFlightLatencyUsSum;
        InFlightCount += other.InFlightCount;
        InFlightLatencyUsMax = Max(InFlightLatencyUsMax, other.InFlightLatencyUsMax);
    }
};

struct TDsProxyInFlightLatencyBucketKey {
    TIntrusivePtr<TStoragePoolCounters> PoolCounters;
    ui32 HandleClass = 0;
    ui32 SizeClassIdx = 0;

    size_t Hash() const {
        return MultiHash(reinterpret_cast<uintptr_t>(PoolCounters.Get()), HandleClass, SizeClassIdx);
    }

    friend bool operator ==(const TDsProxyInFlightLatencyBucketKey& x,
            const TDsProxyInFlightLatencyBucketKey& y) {
        return x.PoolCounters.Get() == y.PoolCounters.Get()
            && x.HandleClass == y.HandleClass
            && x.SizeClassIdx == y.SizeClassIdx;
    }
};

struct TDsProxyInFlightLatencyBucket {
    TDsProxyInFlightLatencyBucketKey Key;
    TDsProxyInFlightLatencyStats Stats;
};

struct TEvDsProxyInFlightLatencySnapshot
    : public TEventLocal<TEvDsProxyInFlightLatencySnapshot, TEvBlobStorage::EvDsProxyInFlightLatencySnapshot>
{
    TVector<TDsProxyInFlightLatencyBucket> Buckets;

    TEvDsProxyInFlightLatencySnapshot() = default;

    explicit TEvDsProxyInFlightLatencySnapshot(TVector<TDsProxyInFlightLatencyBucket>&& buckets)
        : Buckets(std::move(buckets))
    {}
};

IActor* CreateDsProxyInFlightLatencyAggregator();

struct TRequestMonItem {
    ::NMonitoring::TDynamicCounters::TCounterPtr RequestBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr GeneratedSubrequests;
    ::NMonitoring::TDynamicCounters::TCounterPtr GeneratedSubrequestBytes;
    NMonitoring::THistogramPtr ResponseTime;
    TMaxTracker ResponseTimeMax;
    ::NMonitoring::TDynamicCounters::TCounterPtr ResponseTimeUsCompletedSum;
    ::NMonitoring::TDynamicCounters::TCounterPtr ResponseTimeCompletedCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlightResponseTimeUsSum;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlightCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlightResponseTimeUsMax;

    void Init(TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, NPDisk::EDeviceType type) {
        RequestBytes = counters->GetCounter("requestBytes", true);
        GeneratedSubrequests = counters->GetCounter("generatedSubrequests", true);
        GeneratedSubrequestBytes = counters->GetCounter("generatedSubrequestBytes", true);

        NMonitoring::TBucketBounds bounds = GetCommonLatencyHistBounds(type);

        ResponseTime = counters->GetHistogram("responseTimeMs",
            NMonitoring::ExplicitHistogram(std::move(bounds)));
        ResponseTimeMax.Init(counters->GetCounter("responseTimeMsMax", false));
        ResponseTimeUsCompletedSum = counters->GetCounter("responseTimeUsCompletedSum", true);
        ResponseTimeCompletedCount = counters->GetCounter("responseTimeCompletedCount", true);
        InFlightResponseTimeUsSum = counters->GetCounter("inFlightResponseTimeUsSum", false);
        InFlightCount = counters->GetCounter("inFlightCount", false);
        InFlightResponseTimeUsMax = counters->GetCounter("inFlightResponseTimeUsMax", false);
    }

    void Register(ui32 requestBytes, ui32 generatedSubrequests, ui32 generatedSubrequestBytes, double durationSeconds) {
        const double durationMs = durationSeconds * 1000.0;
        const ui64 durationUs = durationSeconds * 1'000'000.0;
        *RequestBytes += requestBytes;
        *GeneratedSubrequests += generatedSubrequests;
        *GeneratedSubrequestBytes += generatedSubrequestBytes;
        ResponseTime->Collect(durationMs);
        ResponseTimeMax.Collect((i64)durationMs);
        ResponseTimeUsCompletedSum->Add(durationUs);
        ResponseTimeCompletedCount->Inc();
    }

    void SetInFlightLatencyStats(const TDsProxyInFlightLatencyStats& stats) {
        InFlightResponseTimeUsSum->Set(stats.InFlightLatencyUsSum);
        InFlightCount->Set(stats.InFlightCount);
        InFlightResponseTimeUsMax->Set(stats.InFlightLatencyUsMax);
    }

    void Update() {
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
    static ui32 SizeClassIndex(EHandleClass handleClass, ui32 requestBytes) {
        if (IsReducedHandleClass(handleClass)) {
            return ReducedSizeClassFromSizeBytes(requestBytes);
        } else {
            return SizeClassFromSizeBytes(requestBytes);
        }
    }

    TRequestMonItem& GetItem(EHandleClass handleClass, ui32 requestBytes) {
        return GetItemBySizeClass(handleClass, SizeClassIndex(handleClass, requestBytes));
    }

    TRequestMonItem& GetItemBySizeClass(EHandleClass handleClass, ui32 sizeClassIdx) {
        Y_ABORT_UNLESS((ui32)handleClass < (ui32)HcCount);
        if (IsReducedHandleClass(handleClass)) {
            Y_ABORT_UNLESS(sizeClassIdx <= MaxReducedSizeClassBucketIdx);
        } else {
            Y_ABORT_UNLESS(sizeClassIdx <= MaxSizeClassBucketIdx);
        }
        return RequestMon[(ui32)handleClass][sizeClassIdx];
    }

    void SetInFlightLatencyStats(EHandleClass handleClass, ui32 sizeClassIdx,
            const TDsProxyInFlightLatencyStats& stats) {
        GetItemBySizeClass(handleClass, sizeClassIdx).SetInFlightLatencyStats(stats);
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

template<>
struct THash<NKikimr::TDsProxyInFlightLatencyBucketKey> {
    size_t operator()(const NKikimr::TDsProxyInFlightLatencyBucketKey& key) const {
        return key.Hash();
    }
};
