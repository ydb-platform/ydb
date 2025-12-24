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

struct TKqpWarmupConfig {
    bool Enabled = false;
    TDuration Deadline = TDuration::Seconds(30);
    ui32 MaxNodesToQuery = 5;
    ui32 MaxConcurrentCompilations = 5;
};

inline TKqpWarmupConfig ImportWarmupConfigFromProto(const NKikimrConfig::TTableServiceConfig::TWarmupConfig& proto) {
    TKqpWarmupConfig config;
    config.Enabled = proto.GetEnabled();
    config.Deadline = TDuration::Seconds(proto.GetDeadlineSeconds());
    config.MaxNodesToQuery = proto.GetMaxNodesToQuery();
    config.MaxConcurrentCompilations = proto.GetMaxConcurrentCompilations();
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
    NActors::TActorId notifyActorId = {});

} // namespace NKikimr::NKqp
