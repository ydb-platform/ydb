#pragma once

#include "defs.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/group_stat.h>
#include <ydb/core/blobstorage/base/common_latency_hist_bounds.h>
#include <ydb/core/mon/mon.h>
 
#include <util/generic/bitops.h>
#include <util/generic/ptr.h>

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Storage pool monitoring counters
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TRequestMonItem {
    NMonitoring::TDynamicCounters::TCounterPtr RequestBytes;
    NMonitoring::TDynamicCounters::TCounterPtr GeneratedSubrequests;
    NMonitoring::TDynamicCounters::TCounterPtr GeneratedSubrequestBytes;
    NMonitoring::THistogramPtr ResponseTime;

    void Init(TIntrusivePtr<NMonitoring::TDynamicCounters> counters, TPDiskCategory::EDeviceType type) { 
        RequestBytes = counters->GetCounter("requestBytes", true);
        GeneratedSubrequests = counters->GetCounter("generatedSubrequests", true);
        GeneratedSubrequestBytes = counters->GetCounter("generatedSubrequestBytes", true);

        NMonitoring::TBucketBounds bounds = GetCommonLatencyHistBounds(type); 

        ResponseTime = counters->GetNamedHistogram("sensor", "responseTimeMs", 
                NMonitoring::ExplicitHistogram(std::move(bounds))); 
    }

    void Register(ui32 requestBytes, ui32 generatedSubrequests, ui32 generatedSubrequestBytes, double durationSeconds) {
        *RequestBytes += requestBytes;
        *GeneratedSubrequests += generatedSubrequests;
        *GeneratedSubrequestBytes += generatedSubrequestBytes;
        ResponseTime->Collect(durationSeconds * 1000.0); 
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

    // Old buckets are: 64 128 256 512 1k 2k 4k 8k 16k 32k 64k 128k 256k 512k 1M 2M 4M 8M 16M -- 19 buckets
    // Buckets are: 256 4k 256k 1M 4M 16M -- 6 buckets
    static constexpr ui32 MaxSizeClassBucketIdx = 5;
    static constexpr const char *const SizeClassNameList[MaxSizeClassBucketIdx + 1] =
      {"256", "4096", "262144", "1048576", "4194304", "16777216"};

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

    static const char* SizeClassName(ui32 sizeClass) {
        return SizeClassNameList[Min<ui32>(MaxSizeClassBucketIdx, sizeClass)];
    }

    TRequestMonItem RequestMon[HcCount][MaxSizeClassBucketIdx + 1];
    TString StoragePoolName;

    TRequestMonItem& GetItem(EHandleClass handleClass, ui32 requestBytes) {
        ui32 sizeClassIdx = SizeClassFromSizeBytes(requestBytes);
        Y_VERIFY((ui32)handleClass < (ui32)HcCount && sizeClassIdx <= MaxSizeClassBucketIdx);
        return RequestMon[(ui32)handleClass][sizeClassIdx];
    }

    TStoragePoolCounters(TIntrusivePtr<NMonitoring::TDynamicCounters> &counters, const TString &storagePoolName, 
            TPDiskCategory::EDeviceType type) { 
        StoragePoolName = storagePoolName;
        TIntrusivePtr<NMonitoring::TDynamicCounters> poolGroup = counters->GetSubgroup("storagePool", storagePoolName);
        for (ui32 handleClass = 0; handleClass < (ui32)HcCount; ++handleClass) {
            TString handleClassName = GetHandleClassName((EHandleClass)handleClass);
            TIntrusivePtr<NMonitoring::TDynamicCounters> hcGroup = poolGroup->GetSubgroup("handleClass", handleClassName);
            for (ui32 sizeClassIdx = 0; sizeClassIdx <= MaxSizeClassBucketIdx; ++sizeClassIdx) {
                TString sizeClassName = SizeClassName(sizeClassIdx);
                RequestMon[handleClass][sizeClassIdx].Init(hcGroup->GetSubgroup("sizeClass", sizeClassName), type); 
            }
        }
    }

};

class TDsProxyPerPoolCounters : public TThrRefBase {
protected:
    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    TMap<TString, TIntrusivePtr<TStoragePoolCounters>> StoragePoolCounters;

public:
    TDsProxyPerPoolCounters(TIntrusivePtr<NMonitoring::TDynamicCounters> counters) {
      TIntrusivePtr<NMonitoring::TDynamicCounters> group = GetServiceCounters(counters, "dsproxynode");
      Counters = group->GetSubgroup("subsystem", "request");
    };

    TIntrusivePtr<TStoragePoolCounters> GetPoolCounters(const TString &storagePoolName, 
            TPDiskCategory::EDeviceType type = TPDiskCategory::DEVICE_TYPE_UNKNOWN) { 
        auto it = StoragePoolCounters.find(storagePoolName);
        if (it != StoragePoolCounters.end()) {
            return it->second;
        }
        TIntrusivePtr<TStoragePoolCounters> spc = MakeIntrusive<TStoragePoolCounters>(Counters, storagePoolName, type); 
        StoragePoolCounters.emplace(storagePoolName, spc);
        return spc;
    }
};


} // NKikimr

