#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/node_whiteboard.pb.h>

#include <ydb/core/base/counters.h>
#include <ydb/core/base/group_stat.h>
#include <ydb/core/util/throughput_meter.h>
#include <ydb/core/mon/mon.h>

#include <library/cpp/monlib/dynamic_counters/percentile/percentile.h>
#include <library/cpp/monlib/metrics/histogram_snapshot.h>

#include <util/generic/ptr.h>

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// BlobStorageProxy node monitoring counters
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TDsProxyNodeMon : public TThrRefBase {
    TIntrusivePtr<NMonitoring::TDynamicCounters> Group;
    TIntrusivePtr<NMonitoring::TDynamicCounters> LatencyOverview;

    NMonitoring::TPercentileTracker<4, 512, 15> PutResponseTime;

    NMonitoring::TPercentileTracker<4, 512, 15> PutTabletLogResponseTime;
    NMonitoring::TPercentileTracker<4, 512, 15> PutTabletLogResponseTime256;
    NMonitoring::TPercentileTracker<4, 512, 15> PutTabletLogResponseTime512;

    static constexpr ui32 KnownDeviceTypesCount = 4;
    using THistoPtrForDeviceType = std::array<NMonitoring::THistogramPtr, KnownDeviceTypesCount>;
    THistoPtrForDeviceType PutTabletLogResponseTimeHist256Ki;
    THistoPtrForDeviceType PutTabletLogResponseTimeHistInf;

    NMonitoring::TPercentileTracker<4, 512, 15> PutAsyncBlobResponseTime;
    THistoPtrForDeviceType PutAsyncBlobResponseTimeHist;
    NMonitoring::TPercentileTracker<4, 512, 15> PutUserDataResponseTime;
    THistoPtrForDeviceType PutUserDataResponseTimeHist;

    NMonitoring::TPercentileTracker<16, 512, 15> GetResponseTime;
    NMonitoring::TPercentileTracker<16, 512, 15> GetAsyncReadResponseTime;
    THistoPtrForDeviceType GetAsyncReadResponseTimeHist;
    NMonitoring::TPercentileTracker<16, 512, 15> GetFastReadResponseTime256Ki;
    THistoPtrForDeviceType GetFastReadResponseTimeHist256Ki;
    NMonitoring::TPercentileTracker<16, 512, 15> GetFastReadResponseTimeInf;
    THistoPtrForDeviceType GetFastReadResponseTimeHistInf;
    NMonitoring::TPercentileTracker<16, 512, 15> GetDiscoverResponseTime;
    THistoPtrForDeviceType GetDiscoverResponseTimeHist;
    NMonitoring::TPercentileTracker<16, 512, 15> GetLowReadResponseTime;
    THistoPtrForDeviceType GetLowReadResponseTimeHist;

    NMonitoring::TPercentileTracker<16, 512, 15> PatchResponseTime;
    THistoPtrForDeviceType PatchResponseTimeHist;

    NMonitoring::TPercentileTracker<16, 512, 15> BlockResponseTime;
    NMonitoring::TPercentileTracker<16, 512, 15> DiscoverResponseTime;
    NMonitoring::TPercentileTracker<16, 512, 15> IndexRestoreGetResponseTime;
    NMonitoring::TPercentileTracker<16, 512, 15> RangeResponseTime;

    std::array<bool, KnownDeviceTypesCount> IsCountersPresentedForIdx;

    // restart counters
    NMonitoring::TDynamicCounters::TCounterPtr RestartPut;
    NMonitoring::TDynamicCounters::TCounterPtr RestartGet;
    NMonitoring::TDynamicCounters::TCounterPtr RestartBlock;
    NMonitoring::TDynamicCounters::TCounterPtr RestartDiscover;
    NMonitoring::TDynamicCounters::TCounterPtr RestartRange;
    NMonitoring::TDynamicCounters::TCounterPtr RestartCollectGarbage;
    NMonitoring::TDynamicCounters::TCounterPtr RestartIndexRestoreGet;
    NMonitoring::TDynamicCounters::TCounterPtr RestartStatus;
    NMonitoring::TDynamicCounters::TCounterPtr RestartPatch;

    std::array<NMonitoring::TDynamicCounters::TCounterPtr, 4> RestartHisto;


    // accelerate counters
    NMonitoring::TDynamicCounters::TCounterPtr AccelerateEvVPutCount;
    NMonitoring::TDynamicCounters::TCounterPtr AccelerateEvVMultiPutCount;
    NMonitoring::TDynamicCounters::TCounterPtr AccelerateEvVGetCount;

    // malfunction counters
    NMonitoring::TDynamicCounters::TCounterPtr EstablishingSessionsTimeout;
    NMonitoring::TDynamicCounters::TCounterPtr EstablishingSessionsTimeout5min;
    NMonitoring::TDynamicCounters::TCounterPtr UnconfiguredTimeout;
    NMonitoring::TDynamicCounters::TCounterPtr UnconfiguredTimeout5min;

    TDsProxyNodeMon(TIntrusivePtr<NMonitoring::TDynamicCounters> &counters, bool initForAllDeviceTypes);
    void CountPutPesponseTime(TPDiskCategory::EDeviceType type, NKikimrBlobStorage::EPutHandleClass cls, ui32 size,
            TDuration duration);
    void CountGetResponseTime(TPDiskCategory::EDeviceType type, NKikimrBlobStorage::EGetHandleClass cls, ui32 size,
            TDuration duration);
    void CountPatchResponseTime(TPDiskCategory::EDeviceType type, TDuration duration);

    // Called only from NodeWarder
    void CheckNodeMonCountersForDeviceType(TPDiskCategory::EDeviceType type);
};

} // NKikimr

