#include "dsproxy_nodemon.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/blobstorage/base/common_latency_hist_bounds.h>

namespace NKikimr {
TDsProxyNodeMon::TDsProxyNodeMon(TIntrusivePtr<::NMonitoring::TDynamicCounters> &counters, bool initForAllDeviceTypes)
        : Group(GetServiceCounters(counters, "dsproxynode"))
{
    TVector<float> percentiles4;
    percentiles4.push_back(0.50f);
    percentiles4.push_back(0.90f);
    percentiles4.push_back(0.95f);
    percentiles4.push_back(1.0f);

    TVector<float> percentiles1;
    percentiles1.push_back(1.0f);

    PutResponseTime.Initialize(Group, "event", "put", "latency", percentiles4);
    PutTabletLogResponseTime.Initialize(Group, "event", "putTabletLogAll", "latency", percentiles1);
    PutTabletLogResponseTime256.Initialize(Group, "event", "putTabletLog256", "latency", percentiles1);
    PutTabletLogResponseTime512.Initialize(Group, "event", "putTabletLog512", "latency", percentiles1);
    PutAsyncBlobResponseTime.Initialize(Group, "event", "putAsyncBlob", "latency", percentiles1);
    PutUserDataResponseTime.Initialize(Group, "event", "putUserData", "latency", percentiles1);

    GetResponseTime.Initialize(Group, "event", "get", "latency", percentiles1);
    GetAsyncReadResponseTime.Initialize(Group, "event", "getAsyncRead", "latency", percentiles1);
    GetFastReadResponseTime256Ki.Initialize(Group, "event", "getFastRead256Ki", "latency", percentiles1);
    GetFastReadResponseTimeInf.Initialize(Group, "event", "getFastReadInf", "latency", percentiles1);
    GetDiscoverResponseTime.Initialize(Group, "event", "getDiscover", "latency", percentiles1);
    GetLowReadResponseTime.Initialize(Group, "event", "getLowRead", "latency", percentiles1);

    BlockResponseTime.Initialize(Group, "event", "block", "latency", percentiles1);
    DiscoverResponseTime.Initialize(Group, "event", "discover", "latency", percentiles1);
    IndexRestoreGetResponseTime.Initialize(Group, "event", "indexRestoreGet", "latency",
            percentiles1);
    RangeResponseTime.Initialize(Group, "event", "range", "latency", percentiles1);
    PatchResponseTime.Initialize(Group, "event", "patch", "latency", percentiles4);

    IsCountersPresentedForIdx.fill(false);
    if (initForAllDeviceTypes) {
        CheckNodeMonCountersForDeviceType(NPDisk::DEVICE_TYPE_ROT);
        CheckNodeMonCountersForDeviceType(NPDisk::DEVICE_TYPE_SSD);
        CheckNodeMonCountersForDeviceType(NPDisk::DEVICE_TYPE_NVME);
        CheckNodeMonCountersForDeviceType(NPDisk::DEVICE_TYPE_UNKNOWN);
    }

    // restart counters
    {
        auto group = Group->GetSubgroup("subsystem", "restart");
        RestartPut = group->GetCounter("EvPut", true);
        RestartGet = group->GetCounter("EvGet", true);
        RestartPatch = group->GetCounter("EvPatch", true);
        RestartBlock = group->GetCounter("EvBlock", true);
        RestartDiscover = group->GetCounter("EvDiscover", true);
        RestartRange = group->GetCounter("EvRange", true);
        RestartCollectGarbage = group->GetCounter("EvCollectGarbage", true);
        RestartIndexRestoreGet = group->GetCounter("EvIndexRestoreGet", true);
        RestartStatus = group->GetCounter("EvStatus", true);
        RestartAssimilate = group->GetCounter("EvAssimilate", true);
    }

    {
        auto group = Group->GetSubgroup("subsystem", "restart_histo");
        auto histoGroup = group->GetSubgroup("sensor", "restart_histo");
        for (size_t i = 0; i < RestartHisto.size(); ++i) {
            RestartHisto[i] = histoGroup->GetNamedCounter("restartCount", ToString(i), true);
        }
    }
    // Accelerate counters
    {
        auto group = Group->GetSubgroup("subsystem", "accelerate");
        AccelerateEvVPutCount = group->GetCounter("EvVPutCount", true);
        AccelerateEvVMultiPutCount = group->GetCounter("EvVMultiPutCount", true);
        AccelerateEvVGetCount = group->GetCounter("EvVGetCount", true);
    }
    // malfunction counters
    {
        auto group = Group->GetSubgroup("subsystem", "malfunction");
        EstablishingSessionsTimeout = group->GetCounter("EstablishingSessionsTimeout", false);
        EstablishingSessionsTimeout5min = group->GetCounter("EstablishingSessionsTimeout5min", false);
        UnconfiguredTimeout = group->GetCounter("UnconfiguredTimeout", false);
        UnconfiguredTimeout5min = group->GetCounter("UnconfiguredTimeout5min", false);
        ConnectedAll = group->GetCounter("ConnectedAll", false);
        ConnectedMinus1 = group->GetCounter("ConnectedMinus1", false);
        ConnectedMinus2 = group->GetCounter("ConnectedMinus2", false);
        ConnectedMinus3more = group->GetCounter("ConnectedMinus3more", false);
    }
    // wipe monitoring counters
    {
        auto group = Group->GetSubgroup("subsystem", "wipemon");
        PutStatusQueries = group->GetCounter("StatusQueries", true);
        IncarnationChanges = group->GetCounter("IncarnationChanges", true);
    }
}

ui32 IdxForType(NPDisk::EDeviceType type) {
    switch (type) {
        case NPDisk::DEVICE_TYPE_ROT: return 0;
        case NPDisk::DEVICE_TYPE_SSD: return 1;
        case NPDisk::DEVICE_TYPE_NVME: return 2;
        case NPDisk::DEVICE_TYPE_UNKNOWN: return 3;
    }
    return 3;
}

void TDsProxyNodeMon::CountPutPesponseTime(NPDisk::EDeviceType type, NKikimrBlobStorage::EPutHandleClass cls,
        ui32 size, TDuration duration) {
    const ui32 durationMs = duration.MilliSeconds();
    const double durationMsFloat = duration.MicroSeconds() / 1000.0;
    PutResponseTime.Increment(durationMs);
    const ui32 idx = IdxForType(type);
    Y_ABORT_UNLESS(IsCountersPresentedForIdx[idx]);
    switch (cls) {
        case NKikimrBlobStorage::EPutHandleClass::TabletLog:
            PutTabletLogResponseTime.Increment(durationMs);
            if (size < (256 << 10)) {
                PutTabletLogResponseTime256.Increment(durationMs);
                Y_DEBUG_ABORT_UNLESS(PutTabletLogResponseTimeHist256Ki[idx]);
                PutTabletLogResponseTimeHist256Ki[idx]->Collect(durationMsFloat);
            } else {
                Y_DEBUG_ABORT_UNLESS(PutTabletLogResponseTimeHistInf[idx]);
                PutTabletLogResponseTimeHistInf[idx]->Collect(durationMsFloat);
                if (size < (512 << 10)) {
                    PutTabletLogResponseTime512.Increment(durationMs);
                }
            }
            break;
        case NKikimrBlobStorage::EPutHandleClass::AsyncBlob:
            PutAsyncBlobResponseTime.Increment(durationMs);
            Y_DEBUG_ABORT_UNLESS(PutAsyncBlobResponseTimeHist[idx]);
            PutAsyncBlobResponseTimeHist[idx]->Collect(durationMsFloat);
            break;
        case NKikimrBlobStorage::EPutHandleClass::UserData:
            PutUserDataResponseTime.Increment(durationMs);
            Y_DEBUG_ABORT_UNLESS(PutUserDataResponseTimeHist[idx]);
            PutUserDataResponseTimeHist[idx]->Collect(durationMsFloat);
            break;
        default:
            Y_ABORT("Unexpected case, HandleClass# %" PRIu64, (ui64)cls);
    }
}

void TDsProxyNodeMon::CountGetResponseTime(NPDisk::EDeviceType type, NKikimrBlobStorage::EGetHandleClass cls,
        ui32 size, TDuration duration) {
    const ui32 durationMs = duration.MilliSeconds();
    const double durationMsFloat = duration.MicroSeconds() / 1000.0;
    GetResponseTime.Increment(durationMs);
    const ui32 idx = IdxForType(type);
    Y_ABORT_UNLESS(IsCountersPresentedForIdx[idx]);
    switch (cls) {
        case NKikimrBlobStorage::EGetHandleClass::AsyncRead:
            GetAsyncReadResponseTime.Increment(durationMs);
            Y_DEBUG_ABORT_UNLESS(GetAsyncReadResponseTimeHist[idx]);
            GetAsyncReadResponseTimeHist[idx]->Collect(durationMsFloat);
            break;
        case NKikimrBlobStorage::EGetHandleClass::FastRead:
            if (size < (256 << 10)) {
                GetFastReadResponseTime256Ki.Increment(durationMs);
                Y_DEBUG_ABORT_UNLESS(GetFastReadResponseTimeHist256Ki[idx]);
                GetFastReadResponseTimeHist256Ki[idx]->Collect(durationMsFloat);
            } else {
                GetFastReadResponseTimeInf.Increment(durationMs);
                Y_DEBUG_ABORT_UNLESS(GetFastReadResponseTimeHistInf[idx]);
                GetFastReadResponseTimeHistInf[idx]->Collect(durationMsFloat);
            }
            break;
        case NKikimrBlobStorage::EGetHandleClass::Discover:
            GetDiscoverResponseTime.Increment(durationMs);
            Y_DEBUG_ABORT_UNLESS(GetDiscoverResponseTimeHist[idx]);
            GetDiscoverResponseTimeHist[idx]->Collect(durationMsFloat);
            break;
        case NKikimrBlobStorage::EGetHandleClass::LowRead:
            GetLowReadResponseTime.Increment(durationMs);
            Y_DEBUG_ABORT_UNLESS(GetLowReadResponseTimeHist[idx]);
            GetLowReadResponseTimeHist[idx]->Collect(durationMsFloat);
            break;
        default:
            Y_ABORT("Unexpected case, HandleClass# %" PRIu64, (ui64)cls);
    }
}

void TDsProxyNodeMon::CountPatchResponseTime(NPDisk::EDeviceType type, TDuration duration) {
    const ui32 durationMs = duration.MilliSeconds();
    const double durationMsFloat = duration.MicroSeconds() / 1000.0;
    PatchResponseTime.Increment(durationMs);
    const ui32 idx = IdxForType(type);
    Y_ABORT_UNLESS(IsCountersPresentedForIdx[idx]);
    PatchResponseTimeHist[idx]->Collect(durationMsFloat);
}

void TDsProxyNodeMon::CheckNodeMonCountersForDeviceType(NPDisk::EDeviceType type) {
    const ui32 idx = IdxForType(type);

    if (!IsCountersPresentedForIdx[idx]) {
        IsCountersPresentedForIdx[idx] = true;
        TIntrusivePtr<::NMonitoring::TDynamicCounters> subGroup =
            Group->GetSubgroup("media", to_lower(NPDisk::DeviceTypeStr(type, true)));

        auto getNamedHisto = [&subGroup, &type] (const TString& name) {
            auto buckets = NMonitoring::ExplicitHistogram(GetCommonLatencyHistBounds(type));
            return subGroup->GetHistogram(name, std::move(buckets));
        };

        PutTabletLogResponseTimeHist256Ki[idx] = getNamedHisto("putTabletLog256KiMs");
        PutTabletLogResponseTimeHistInf[idx] = getNamedHisto("putTabletLogInfMs");
        PutAsyncBlobResponseTimeHist[idx] = getNamedHisto("putAsyncBlobMs");
        PutUserDataResponseTimeHist[idx] = getNamedHisto("putUserDataMs");
        GetAsyncReadResponseTimeHist[idx] = getNamedHisto("getAsyncReadMs");
        GetFastReadResponseTimeHist256Ki[idx] = getNamedHisto("getFastRead256KiMs");
        GetFastReadResponseTimeHistInf[idx] = getNamedHisto("getFastReadInfMs");
        GetDiscoverResponseTimeHist[idx] = getNamedHisto("getDiscoverMs");
        GetLowReadResponseTimeHist[idx] = getNamedHisto("getLowReadMs");
        PatchResponseTimeHist[idx] = getNamedHisto("patchMs");
    }
}
} // NKikimr
