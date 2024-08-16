#pragma once

#include "defs.h"

#include "dsproxy_nodemon.h"
#include "dsproxy_timestats.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/storagepoolmon/storagepool_counters.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/protos/node_whiteboard.pb.h>
#include <ydb/core/util/throughput_meter.h>
#include <ydb/core/mon/mon.h>
#include <library/cpp/monlib/dynamic_counters/percentile/percentile_lg.h>
#include <util/generic/ptr.h>

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// BlobStorageProxy monitoring counters
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum class ERequestType {
    Get,
    Put,
    Discover,
    Range,
    Patch,
    CollectGarbage,
    Status,
    Assimilate,
    Block,
};

struct TRequestMonGroup {
    ::NMonitoring::TDynamicCounters::TCounterPtr VGetBlobsIssued;
    ::NMonitoring::TDynamicCounters::TCounterPtr VGetRangesIssued;
    ::NMonitoring::TDynamicCounters::TCounterPtr VGetBlobsReturnedWithData;
    ::NMonitoring::TDynamicCounters::TCounterPtr VGetBlobsReturnedWithNoData;
    ::NMonitoring::TDynamicCounters::TCounterPtr VGetBlobsReturnedWithErrors;
    ::NMonitoring::TDynamicCounters::TCounterPtr VPutBlobsIssued;
    ::NMonitoring::TDynamicCounters::TCounterPtr VMovedPatchBlobsIssued;

    void Init(const TIntrusivePtr<::NMonitoring::TDynamicCounters> &group) {
        VGetBlobsIssued = group->GetCounter("VGetBlobsIssued", true);
        VGetRangesIssued = group->GetCounter("VGetRangesIssued", true);
        VGetBlobsReturnedWithData = group->GetCounter("VGetBlobsReturnedWithData", true);
        VGetBlobsReturnedWithNoData = group->GetCounter("VGetBlobsReturnedWithNoData", true);
        VGetBlobsReturnedWithErrors = group->GetCounter("VGetBlobsReturnedWithErrors", true);
        VPutBlobsIssued = group->GetCounter("VPutBlobsIssued", true);
        VMovedPatchBlobsIssued = group->GetCounter("VMovedPatchBlobsIssued", true);
    }

    void CountEvent(IEventBase *ev, ui32 type) {
        switch (type) {
            case TEvBlobStorage::EvPut:
                VPutBlobsIssued->Inc();
                break;

            case TEvBlobStorage::EvVMultiPut:
                *VPutBlobsIssued += static_cast<TEvBlobStorage::TEvVMultiPut&>(*ev).Record.ItemsSize();
                break;

            case TEvBlobStorage::EvVMovedPatch:
                VMovedPatchBlobsIssued->Inc();
                break;

            case TEvBlobStorage::EvVGet: {
                const auto& record = static_cast<TEvBlobStorage::TEvVGet&>(*ev).Record;
                *VGetBlobsIssued += record.ExtremeQueriesSize();
                *VGetRangesIssued += record.HasRangeQuery();
                break;
            }

            case TEvBlobStorage::EvVGetResult: {
                const auto& record = static_cast<TEvBlobStorage::TEvVGetResult&>(*ev).Record;
                if (record.GetStatus() != NKikimrProto::OK) {
                    *VGetBlobsReturnedWithErrors += record.ResultSize();
                } else {
                    for (const NKikimrBlobStorage::TQueryResult& result : record.GetResult()) {
                        switch (result.GetStatus()) {
                            case NKikimrProto::OK:
                                ++*VGetBlobsReturnedWithData;
                                break;

                            case NKikimrProto::NODATA:
                            case NKikimrProto::NOT_YET:
                                ++*VGetBlobsReturnedWithNoData;
                                break;

                            default:
                                ++*VGetBlobsReturnedWithErrors;
                                break;
                        }
                    }
                }
                break;
            }
        }
    }
};

struct TResponseStatusGroup : TThrRefBase {
#define ENUM_STATUS(XX) \
    XX(OK) \
    XX(ERROR) \
    XX(DEADLINE) \
    XX(RACE) \
    XX(BLOCKED) \
    XX(ALREADY) \
    XX(NODATA) \
    // END

#define XX(NAME) ::NMonitoring::TDynamicCounters::TCounterPtr Num##NAME;
    ENUM_STATUS(XX)
#undef XX

    TResponseStatusGroup(const ::NMonitoring::TDynamicCounterPtr& group)
        : TThrRefBase()
#define XX(NAME) , Num##NAME(group->GetCounter(#NAME, true))
        ENUM_STATUS(XX)
#undef XX
    {}

    void Account(NKikimrProto::EReplyStatus status) {
        switch (status) {
#define XX(NAME) \
            case NKikimrProto::NAME: \
                ++*Num##NAME; \
                break;
            ENUM_STATUS(XX)
#undef XX
            default:
                Y_ABORT("unexpected Status# %s", NKikimrProto::EReplyStatus_Name(status).data());
        }
    }
#undef ENUM_STATUS
};

class TBlobStorageGroupProxyMon : public TThrRefBase {
public:
    TIntrusivePtr<TDsProxyNodeMon> NodeMon;

protected:
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> PercentileCounters;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> ResponseGroup;
    ui64 GroupIdGen = Max<ui64>(); // group id:group gen
    std::atomic<bool> IsLimitedMon = ATOMIC_VAR_INIT(true);

    TIntrusivePtr<::NMonitoring::TDynamicCounters> ThroughputGroup;
    std::unique_ptr<TThroughputMeter> PutTabletLogThroughput;
    std::unique_ptr<TThroughputMeter> PutAsyncBlobThroughput;
    std::unique_ptr<TThroughputMeter> PutUserDataThroughput;
    std::unique_ptr<TThroughputMeter> PutThroughput;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> LatencyOverviewGroup;


    // log response time
    NMonitoring::TPercentileTrackerLg<3, 4, 3> PutResponseTime; // Used by whiteboard

    NMonitoring::TPercentileTrackerLg<3, 4, 3> PutTabletLogResponseTime;
    NMonitoring::TPercentileTrackerLg<3, 4, 3> PutTabletLogResponseTime256;
    NMonitoring::TPercentileTrackerLg<3, 4, 3> PutTabletLogResponseTime512;

    NMonitoring::TPercentileTrackerLg<3, 4, 3> PutAsyncBlobResponseTime;
    NMonitoring::TPercentileTrackerLg<3, 4, 3> PutUserDataResponseTime;

    NMonitoring::TPercentileTrackerLg<3, 4, 3> GetResponseTime; // Used by witheboard

    NMonitoring::TPercentileTrackerLg<3, 4, 3> BlockResponseTime;
    NMonitoring::TPercentileTrackerLg<3, 4, 3> DiscoverResponseTime;
    NMonitoring::TPercentileTrackerLg<3, 4, 3> IndexRestoreGetResponseTime;
    NMonitoring::TPercentileTrackerLg<3, 4, 3> RangeResponseTime;
    NMonitoring::TPercentileTrackerLg<3, 4, 3> PatchResponseTime;

    // event counters
    TIntrusivePtr<::NMonitoring::TDynamicCounters> EventGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr EventPut;
    ::NMonitoring::TDynamicCounters::TCounterPtr EventPutBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr EventGetResBytes;
    TMap<ui32, ::NMonitoring::TDynamicCounters::TCounterPtr> EventPutBytesBuckets;


    // handoff use reason
    TIntrusivePtr<::NMonitoring::TDynamicCounters> HandoffGroup;

    // subevent counters
    TRequestMonGroup GetGroup;
    TRequestMonGroup PutGroup;
    TRequestMonGroup DiscoverGroup;
    TRequestMonGroup RangeGroup;
    TRequestMonGroup PatchGroup;
    TRequestMonGroup CollectGarbageGroup;
    TRequestMonGroup StatusGroup;
    TRequestMonGroup AssimilateGroup;
    TRequestMonGroup BlockGroup;

public:
    TBlobStorageGroupProxyTimeStats TimeStats;

    // handoff use reason
    std::array<::NMonitoring::TDynamicCounters::TCounterPtr, 8> HandoffPartsSent;

    TAtomic PutSamplePPM = 0;
    TAtomic GetSamplePPM = 0;
    TAtomic DiscoverSamplePPM = 0;

    // event counters
    ::NMonitoring::TDynamicCounters::TCounterPtr EventGet;
    ::NMonitoring::TDynamicCounters::TCounterPtr EventBlock;
    ::NMonitoring::TDynamicCounters::TCounterPtr EventDiscover;
    ::NMonitoring::TDynamicCounters::TCounterPtr EventRange;
    ::NMonitoring::TDynamicCounters::TCounterPtr EventCollectGarbage;
    ::NMonitoring::TDynamicCounters::TCounterPtr EventMultiGet;
    ::NMonitoring::TDynamicCounters::TCounterPtr EventIndexRestoreGet;
    ::NMonitoring::TDynamicCounters::TCounterPtr EventMultiCollect;
    ::NMonitoring::TDynamicCounters::TCounterPtr EventStatus;
    ::NMonitoring::TDynamicCounters::TCounterPtr EventStopPutBatching;
    ::NMonitoring::TDynamicCounters::TCounterPtr EventStopGetBatching;
    ::NMonitoring::TDynamicCounters::TCounterPtr EventPatch;
    ::NMonitoring::TDynamicCounters::TCounterPtr EventAssimilate;

    ::NMonitoring::TDynamicCounters::TCounterPtr PutsSentViaPutBatching;
    ::NMonitoring::TDynamicCounters::TCounterPtr PutBatchesSent;

    // active event counters
    TIntrusivePtr<::NMonitoring::TDynamicCounters> ActiveRequestsGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr ActivePut;
    ::NMonitoring::TDynamicCounters::TCounterPtr ActivePutCapacity;
    ::NMonitoring::TDynamicCounters::TCounterPtr ActiveGet;
    ::NMonitoring::TDynamicCounters::TCounterPtr ActiveGetCapacity;
    ::NMonitoring::TDynamicCounters::TCounterPtr ActiveBlock;
    ::NMonitoring::TDynamicCounters::TCounterPtr ActiveDiscover;
    ::NMonitoring::TDynamicCounters::TCounterPtr ActiveRange;
    ::NMonitoring::TDynamicCounters::TCounterPtr ActiveCollectGarbage;
    ::NMonitoring::TDynamicCounters::TCounterPtr ActiveMultiGet;
    ::NMonitoring::TDynamicCounters::TCounterPtr ActiveIndexRestoreGet;
    ::NMonitoring::TDynamicCounters::TCounterPtr ActiveMultiCollect;
    ::NMonitoring::TDynamicCounters::TCounterPtr ActiveStatus;
    ::NMonitoring::TDynamicCounters::TCounterPtr ActivePatch;
    ::NMonitoring::TDynamicCounters::TCounterPtr ActiveAssimilate;

    std::optional<TResponseStatusGroup> RespStatPut;
    std::optional<TResponseStatusGroup> RespStatGet;
    std::optional<TResponseStatusGroup> RespStatBlock;
    std::optional<TResponseStatusGroup> RespStatDiscover;
    std::optional<TResponseStatusGroup> RespStatRange;
    std::optional<TResponseStatusGroup> RespStatCollectGarbage;
    std::optional<TResponseStatusGroup> RespStatStatus;
    std::optional<TResponseStatusGroup> RespStatPatch;
    std::optional<TResponseStatusGroup> RespStatAssimilate;

    // special patch counters
    ::NMonitoring::TDynamicCounters::TCounterPtr VPatchContinueFailed;
    ::NMonitoring::TDynamicCounters::TCounterPtr VPatchPartPlacementVerifyFailed;
    ::NMonitoring::TDynamicCounters::TCounterPtr PatchesWithFallback;

    TRequestMonGroup& GetRequestMonGroup(ERequestType request) {
        switch (request) {
            case ERequestType::Get: return GetGroup;
            case ERequestType::Put: return PutGroup;
            case ERequestType::Discover: return DiscoverGroup;
            case ERequestType::Range: return RangeGroup;
            case ERequestType::Patch: return PatchGroup;
            case ERequestType::CollectGarbage: return CollectGarbageGroup;
            case ERequestType::Status: return StatusGroup;
            case ERequestType::Assimilate: return AssimilateGroup;
            case ERequestType::Block: return BlockGroup;
        }
        Y_ABORT();
    }

    void CountEvent(ERequestType request, IEventBase *ev, ui32 type) {
        GetRequestMonGroup(request).CountEvent(ev, type);
    }

    TBlobStorageGroupProxyMon(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
            const TIntrusivePtr<::NMonitoring::TDynamicCounters>& percentileCounters,
            const TIntrusivePtr<::NMonitoring::TDynamicCounters>& overviewCounters,
            const TIntrusivePtr<TBlobStorageGroupInfo>& info,
            const TIntrusivePtr<TDsProxyNodeMon> &nodeMon,
            bool isLimitedMon);

    bool GetGroupIdGen(ui32 *groupId, ui32 *groupGen) const;

    void BecomeFull();

    void SerializeToWhiteboard(NKikimrWhiteboard::TBSGroupStateInfo& pb, ui32 groupId) const;

    void CountPutEvent(ui32 size) {
        ++*EventPut;
        *EventPutBytes += size;
        auto bucketIt = EventPutBytesBuckets.upper_bound(size);
        Y_ABORT_UNLESS(bucketIt != EventPutBytesBuckets.begin());
        ++*std::prev(bucketIt)->second;
    }

    void CountThroughput(NKikimrBlobStorage::EPutHandleClass cls, ui32 size) {
        switch (cls) {
            case NKikimrBlobStorage::EPutHandleClass::TabletLog:
                PutTabletLogThroughput->Count(size);
                break;
            case NKikimrBlobStorage::EPutHandleClass::AsyncBlob:
                PutAsyncBlobThroughput->Count(size);
                break;
            case NKikimrBlobStorage::EPutHandleClass::UserData:
                PutUserDataThroughput->Count(size);
                break;
        }
        PutThroughput->Count(size);
    }

    void CountPutPesponseTime(NPDisk::EDeviceType type, NKikimrBlobStorage::EPutHandleClass cls, ui32 size,
            TDuration duration) {
        const ui32 durationMs = duration.MilliSeconds();
        PutResponseTime.Increment(durationMs);
        switch (cls) {
            case NKikimrBlobStorage::EPutHandleClass::TabletLog:
                PutTabletLogResponseTime.Increment(durationMs);
                if (size < (256 << 10)) {
                    PutTabletLogResponseTime256.Increment(durationMs);
                } else if (size < (512 << 10)) {
                    PutTabletLogResponseTime512.Increment(durationMs);
                }
                break;
            case NKikimrBlobStorage::EPutHandleClass::AsyncBlob:
                PutAsyncBlobResponseTime.Increment(durationMs);
                break;
            case NKikimrBlobStorage::EPutHandleClass::UserData:
                PutUserDataResponseTime.Increment(durationMs);
                break;
            default:
                Y_ABORT("Unexpected case, HandleClass# %" PRIu64, (ui64)cls);
        }
        NodeMon->CountPutPesponseTime(type, cls, size, duration);
    }

    void CountGetResponseTime(NPDisk::EDeviceType type, NKikimrBlobStorage::EGetHandleClass cls, ui32 size,
            TDuration duration) {
        *EventGetResBytes += size;
        GetResponseTime.Increment(duration.MilliSeconds());
        NodeMon->CountGetResponseTime(type, cls, size, duration);
    }

    void CountBlockResponseTime(TDuration duration) {
        BlockResponseTime.Increment(duration.MilliSeconds());
        NodeMon->BlockResponseTime.Increment(duration.MilliSeconds());
    }

    void CountDiscoverResponseTime(TDuration duration) {
        DiscoverResponseTime.Increment(duration.MilliSeconds());
        NodeMon->DiscoverResponseTime.Increment(duration.MilliSeconds());
    }

    void CountIndexRestoreGetResponseTime(TDuration duration) {
        IndexRestoreGetResponseTime.Increment(duration.MilliSeconds());
        NodeMon->IndexRestoreGetResponseTime.Increment(duration.MilliSeconds());
    }

    void CountRangeResponseTime(TDuration duration) {
        RangeResponseTime.Increment(duration.MilliSeconds());
        NodeMon->RangeResponseTime.Increment(duration.MilliSeconds());
    }

    void CountPatchResponseTime(NPDisk::EDeviceType type, TDuration duration) {
        PatchResponseTime.Increment(duration.MilliSeconds());
        NodeMon->CountPatchResponseTime(type, duration);
    }

    void Update();
    void ThroughputUpdate();
};

} // NKikimr

