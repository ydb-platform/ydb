#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/core/base/events.h>
#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/protos/table_service_config.pb.h>

#include <util/datetime/base.h>

namespace NKikimr::NKqp {

struct TEvKqpWarmupComplete : public NActors::TEventLocal<TEvKqpWarmupComplete, TKqpEvents::EvWarmupComplete> {
    bool Success;
    TString Message;
    ui32 EntriesLoaded;

    TEvKqpWarmupComplete(bool success, TString message = {}, ui32 entriesLoaded = 0)
        : Success(success)
        , Message(std::move(message))
        , EntriesLoaded(entriesLoaded)
    {}
};

struct TEvStartWarmup : public NActors::TEventLocal<TEvStartWarmup, TKqpEvents::EvStartWarmup> {
    ui32 DiscoveredNodesCount;
    TVector<ui32> NodeIds;

    explicit TEvStartWarmup(ui32 nodesCount, TVector<ui32> nodeIds = {})
        : DiscoveredNodesCount(nodesCount)
        , NodeIds(std::move(nodeIds))
    {}
};

struct TKqpWarmupConfig {
    TDuration Deadline = TDuration::Seconds(10);        // Soft deadline: time for discovery + compilation after warmup actor start
    TDuration HardDeadline = TDuration::Seconds(20);    // Hard deadline: max time from actor start (must be >= Deadline)
    ui32 MaxConcurrentCompilations = 5;
    ui32 MaxQueriesToLoad = 1000;
    ui32 MaxNodesToRequest = 5;                           // Max nodes to query for warmup (0 = all nodes)
};

inline TKqpWarmupConfig ImportWarmupConfigFromProto(const NKikimrConfig::TTableServiceConfig::TCompileCacheWarmupConfig& proto) {
    TKqpWarmupConfig config;
    config.Deadline = TDuration::Seconds(proto.GetSoftDeadlineSeconds());
    config.HardDeadline = TDuration::Seconds(proto.GetHardDeadlineSeconds());
    config.MaxConcurrentCompilations = proto.GetMaxConcurrentCompilations();
    config.MaxQueriesToLoad = proto.GetMaxQueriesToLoad();
    config.MaxNodesToRequest = proto.GetMaxNodesToRequest();
    return config;
}

inline NActors::TActorId MakeKqpWarmupActorId(ui32 nodeId) {
    const char name[12] = "kqp_warmup";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

NActors::IActor* CreateKqpWarmupActor(
    const TKqpWarmupConfig& config,
    const TString& database = {},
    const TString& cluster = {},
    TVector<NActors::TActorId> notifyActorIds = {});

} // namespace NKikimr::NKqp
