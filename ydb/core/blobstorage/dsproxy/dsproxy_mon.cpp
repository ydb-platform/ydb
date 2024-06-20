#include "dsproxy_mon.h"

static const TVector<float> Percentiles1 = {1.0f};
static const TVector<float> Percentiles4 = {0.5f, 0.9f, 0.95f, 1.0f};

namespace NKikimr {

TBlobStorageGroupProxyMon::TBlobStorageGroupProxyMon(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& percentileCounters,
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& overviewCounters,
        const TIntrusivePtr<TBlobStorageGroupInfo>& info,
        const TIntrusivePtr<TDsProxyNodeMon> &nodeMon,
        bool constructLimited)
    : NodeMon(nodeMon)
    , Counters(counters)
    , PercentileCounters(percentileCounters)
    , ResponseGroup(percentileCounters->GetSubgroup("subsystem", "response"))
    , LatencyOverviewGroup(overviewCounters->GetSubgroup("subsystem", "latency"))
    , EventGroup(Counters->GetSubgroup("subsystem", "event"))
    , HandoffGroup(Counters->GetSubgroup("subsystem", "handoff"))
    , ActiveRequestsGroup(Counters->GetSubgroup("subsystem", "requests"))
{
    if (info) {
        const TBlobStorageGroupInfo::TDynamicInfo& dyn = info->GetDynamicInfo();
        GroupIdGen = (ui64(dyn.GroupId.GetRawId()) << 32) | dyn.GroupGeneration;
    }

    BlockResponseTime.Initialize(ResponseGroup, "event", "block", "Response in millisec", Percentiles1);

    if (!constructLimited) {
        BecomeFull();
    }

    // event counters
    EventPut = EventGroup->GetCounter("EvPut", true);
    EventPutBytes = EventGroup->GetCounter("EvPutBytes", true);
    EventGet = EventGroup->GetCounter("EvGet", true);
    EventGetResBytes = EventGroup->GetCounter("EvGetResBytes", true);
    EventBlock = EventGroup->GetCounter("EvBlock", true);
    EventDiscover = EventGroup->GetCounter("EvDiscover", true);
    EventRange = EventGroup->GetCounter("EvRange", true);
    EventCollectGarbage = EventGroup->GetCounter("EvCollectGarbage", true);
    EventMultiGet = EventGroup->GetCounter("EvMultiGet", true);
    EventIndexRestoreGet = EventGroup->GetCounter("EvIndexRestoreGet", true);
    EventMultiCollect = EventGroup->GetCounter("EvMultiCollect", true);
    EventStatus = EventGroup->GetCounter("EvStatus", true);
    EventStopPutBatching = EventGroup->GetCounter("EvStopPutBatching", true);
    EventStopGetBatching = EventGroup->GetCounter("EvStopGetBatching", true);
    EventPatch = EventGroup->GetCounter("EvPatch", true);
    EventAssimilate = EventGroup->GetCounter("EvAssimilate", true);

    PutsSentViaPutBatching = EventGroup->GetCounter("PutsSentViaPutBatching", true);
    PutBatchesSent = EventGroup->GetCounter("PutBatchesSent", true);

    auto buckets = EventGroup->GetSubgroup("sensor", "EvPutBytesBuckets");
    for (ui32 size : {0, 256, 4096, 65536, 250000, 1000000, 4000000}) {
        EventPutBytesBuckets.emplace(size, buckets->GetNamedCounter("size", Sprintf("%" PRIu32, (ui32)size), true));
    }

    // handoff use reason
    for (size_t i = 0; i < HandoffPartsSent.size(); ++i) {
        HandoffPartsSent[i] = HandoffGroup->GetSubgroup("handoffPartsSent", Sprintf("%zu", i))->GetCounter("events", true);
    }

    // active requests
    ActivePut = ActiveRequestsGroup->GetCounter("ActivePut");
    ActivePutCapacity = ActiveRequestsGroup->GetCounter("ActivePutCapacity");
    ActiveGet = ActiveRequestsGroup->GetCounter("ActiveGet");
    ActiveGetCapacity = ActiveRequestsGroup->GetCounter("ActiveGetCapacity");
    ActiveBlock = ActiveRequestsGroup->GetCounter("ActiveBlock");
    ActiveDiscover = ActiveRequestsGroup->GetCounter("ActiveDiscover");
    ActiveRange = ActiveRequestsGroup->GetCounter("ActiveRange");
    ActiveCollectGarbage = ActiveRequestsGroup->GetCounter("ActiveCollectGarbage");
    ActiveStatus = ActiveRequestsGroup->GetCounter("ActiveStatus");
    ActivePatch = ActiveRequestsGroup->GetCounter("ActivePatch");
    ActiveAssimilate = ActiveRequestsGroup->GetCounter("ActiveAssimilate");

    // special patch counters
    VPatchContinueFailed = ActiveRequestsGroup->GetCounter("VPatchContinueFailed");
    VPatchPartPlacementVerifyFailed = ActiveRequestsGroup->GetCounter("VPatchPartPlacementVerifyFailed");
    PatchesWithFallback = ActiveRequestsGroup->GetCounter("PatchesWithFallback");

    // subevents
    {
        auto group = Counters->GetSubgroup("subsystem", "subevents");
        GetGroup.Init(group->GetSubgroup("request", "get"));
        PutGroup.Init(group->GetSubgroup("request", "put"));
        DiscoverGroup.Init(group->GetSubgroup("request", "discover"));
        RangeGroup.Init(group->GetSubgroup("request", "range"));
        PatchGroup.Init(group->GetSubgroup("request", "patch"));
    }

    ActiveMultiGet = ActiveRequestsGroup->GetCounter("ActiveMultiGet");
    ActiveIndexRestoreGet = ActiveRequestsGroup->GetCounter("ActiveIndexRestoreGet");
    ActiveMultiCollect = ActiveRequestsGroup->GetCounter("ActiveMultiCollect");

    auto respStatGroup = NodeMon->Group->GetSubgroup("subsystem", "responseStatus");
    RespStatPut.emplace(respStatGroup->GetSubgroup("request", "put"));
    RespStatGet.emplace(respStatGroup->GetSubgroup("request", "get"));
    RespStatBlock.emplace(respStatGroup->GetSubgroup("request", "block"));
    RespStatDiscover.emplace(respStatGroup->GetSubgroup("request", "discover"));
    RespStatRange.emplace(respStatGroup->GetSubgroup("request", "range"));
    RespStatCollectGarbage.emplace(respStatGroup->GetSubgroup("request", "collectGarbage"));
    RespStatStatus.emplace(respStatGroup->GetSubgroup("request", "status"));
    RespStatPatch.emplace(respStatGroup->GetSubgroup("request", "patch"));
    RespStatAssimilate.emplace(respStatGroup->GetSubgroup("request", "assimilate"));
}

void TBlobStorageGroupProxyMon::BecomeFull() {
    if (IsLimitedMon) {
        ThroughputGroup = PercentileCounters->GetSubgroup("subsystem", "throughput");
        PutTabletLogThroughput.reset(new TThroughputMeter(4, ThroughputGroup, "event", "putTabletLog", "bytes per second", Percentiles4));
        PutAsyncBlobThroughput.reset(new TThroughputMeter(4, ThroughputGroup, "event", "putAsyncBlob", "bytes per second", Percentiles4));
        PutUserDataThroughput.reset(new TThroughputMeter(4, ThroughputGroup, "event", "putUserData", "bytes per second", Percentiles4));
        PutThroughput.reset(new TThroughputMeter(4, ThroughputGroup, "event", "any", "bytes per second", Percentiles4));

        PutResponseTime.Initialize(ResponseGroup, "event", "put", "Response in millisec", Percentiles4);

        TIntrusivePtr<::NMonitoring::TDynamicCounters> putTabletLogGroup =
            ResponseGroup->GetSubgroup("event", "putTabletLog");

        PutTabletLogResponseTime.Initialize(ResponseGroup, "event", "putTabletLogAll", "ms", Percentiles1);

        PutTabletLogResponseTime256.Initialize(putTabletLogGroup, "size", "256", "Response in millisec", Percentiles1);
        PutTabletLogResponseTime512.Initialize(putTabletLogGroup, "size", "512", "Response in millisec", Percentiles1);

        PutAsyncBlobResponseTime.Initialize(ResponseGroup, "event", "putAsyncBlob", "Response in millisec", Percentiles1);
        PutUserDataResponseTime.Initialize(ResponseGroup, "event", "putUserData", "Response in millisec", Percentiles1);

        GetResponseTime.Initialize(ResponseGroup, "event", "get", "Response in millisec", Percentiles1);

        DiscoverResponseTime.Initialize(ResponseGroup, "event", "discover", "Response in millisec", Percentiles1);
        IndexRestoreGetResponseTime.Initialize(ResponseGroup, "event", "indexRestoreGet", "Response in millisec",
                Percentiles1);
        RangeResponseTime.Initialize(ResponseGroup, "event", "range", "Response in millisec", Percentiles1);
        PatchResponseTime.Initialize(ResponseGroup, "event", "patch", "Response in millisec", Percentiles1);
    }
    IsLimitedMon = false;
}

void TBlobStorageGroupProxyMon::SerializeToWhiteboard(NKikimrWhiteboard::TBSGroupStateInfo& pb, ui32 groupId) const {
    NKikimrWhiteboard::EFlag flag = NKikimrWhiteboard::EFlag::Green;

    auto calculate = [&flag](const auto& tracker) {
        for (const auto& x : tracker.Percentiles) {
            const float percentile = x.first;
            const ui32 milliseconds = *x.second;
            if (percentile <= 0.95) {
                if (milliseconds >= 2000 && flag < NKikimrWhiteboard::EFlag::Red) {
                    flag = NKikimrWhiteboard::EFlag::Red;
                } else if (milliseconds >= 1000 && flag < NKikimrWhiteboard::EFlag::Orange) {
                    flag = NKikimrWhiteboard::EFlag::Orange;
                } else if (milliseconds >= 500 && flag < NKikimrWhiteboard::EFlag::Yellow) {
                    flag = NKikimrWhiteboard::EFlag::Yellow;
                }
            }
        }
    };

    if (!IsLimitedMon) {
        calculate(PutResponseTime);
        calculate(GetResponseTime);
    }
    pb.SetGroupID(groupId);
    pb.SetLatency(flag);
}

bool TBlobStorageGroupProxyMon::GetGroupIdGen(ui32 *groupId, ui32 *groupGen) const {
    ui64 value = GroupIdGen;
    if (value != Max<ui64>()) {
        const ui64 mask = Max<ui32>();
        *groupId = (value >> 32) & mask;
        *groupGen = value & mask;
        return true;
    } else {
        return false;
    }
}

void TBlobStorageGroupProxyMon::Update() {
    if (!IsLimitedMon) {
        PutResponseTime.Update();

        PutTabletLogResponseTime.Update();
        PutTabletLogResponseTime256.Update();
        PutTabletLogResponseTime512.Update();

        PutAsyncBlobResponseTime.Update();
        PutUserDataResponseTime.Update();

        GetResponseTime.Update();

        DiscoverResponseTime.Update();
        IndexRestoreGetResponseTime.Update();
        RangeResponseTime.Update();
        PatchResponseTime.Update();
    }

    BlockResponseTime.Update();
}

void TBlobStorageGroupProxyMon::ThroughputUpdate() {
    if (!IsLimitedMon) {
        for (auto *sensor : {&PutTabletLogThroughput, &PutAsyncBlobThroughput, &PutUserDataThroughput, &PutThroughput}) {
            sensor->get()->UpdateHistogram();
        }
    }
}


} // NKikimr

