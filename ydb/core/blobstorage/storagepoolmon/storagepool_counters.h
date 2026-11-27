#pragma once

#include "defs.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/group_stat.h>
#include <ydb/core/blobstorage/base/common_latency_hist_bounds.h>
#include <ydb/core/mon/mon.h>

#include <util/generic/bitops.h>
#include <util/generic/ptr.h>
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

    void Init(TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, NPDisk::EDeviceType type) {
        RequestBytes = counters->GetCounter("requestBytes", true);
        GeneratedSubrequests = counters->GetCounter("generatedSubrequests", true);
        GeneratedSubrequestBytes = counters->GetCounter("generatedSubrequestBytes", true);

        NMonitoring::TBucketBounds bounds = GetCommonLatencyHistBounds(type);

        ResponseTime = counters->GetHistogram("responseTimeMs",
            NMonitoring::ExplicitHistogram(std::move(bounds)));
        ResponseTimeMax.Init(counters->GetCounter("responseTimeMsMax", false));
    }

    void Register(ui32 requestBytes, ui32 generatedSubrequests, ui32 generatedSubrequestBytes, double durationSeconds) {
        *RequestBytes += requestBytes;
        *GeneratedSubrequests += generatedSubrequests;
        *GeneratedSubrequestBytes += generatedSubrequestBytes;
        ResponseTime->Collect(durationSeconds * 1000.0);
        ResponseTimeMax.Collect((i64)(durationSeconds * 1000.0));
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

    static const char* GetHandleClassName(EHandleClass handleClass) {
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

private:
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
    static ui32 SizeClassFromSizeBytes(ui64 requestBytes) {
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
    static ui32 ReducedSizeClassFromSizeBytes(ui64 requestBytes) {
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
    static ui32 GetSizeClass(EHandleClass handleClass, ui64 bytes) {
        return IsReducedHandleClass(handleClass) ? ReducedSizeClassFromSizeBytes(bytes) : SizeClassFromSizeBytes(bytes);
    }

    static const char* GetSizeClassName(EHandleClass handleClass, ui32 index) {
        return IsReducedHandleClass(handleClass) ? ReducedSizeClassName(index) : SizeClassName(index);
    }

    TRequestMonItem& GetItem(EHandleClass handleClass, ui32 requestBytes) {
        Y_ABORT_UNLESS((ui32)handleClass < (ui32)HcCount);
        return RequestMon[handleClass][GetSizeClass(handleClass, requestBytes)];
    }

    TStoragePoolCounters(TIntrusivePtr<::NMonitoring::TDynamicCounters> &counters, const TString &storagePoolName,
            NPDisk::EDeviceType type)
        : StoragePoolName(storagePoolName)
        , PoolGroup(counters->GetSubgroup("storagePool", storagePoolName))
    {
        for (ui32 handleClass = 0; handleClass < (ui32)HcCount; ++handleClass) {
            const auto hc = static_cast<EHandleClass>(handleClass);
            const auto hcGroup = PoolGroup->GetSubgroup("handleClass", GetHandleClassName(hc));
            const ui32 maxIdx = IsReducedHandleClass(hc) ? MaxReducedSizeClassBucketIdx : MaxSizeClassBucketIdx;
            for (ui32 idx = 0; idx <= maxIdx; ++idx) {
                RequestMon[handleClass][idx].Init(hcGroup->GetSubgroup("sizeClass", GetSizeClassName(hc, idx)), type);
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

