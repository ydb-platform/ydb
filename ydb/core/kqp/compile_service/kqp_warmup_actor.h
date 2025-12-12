#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/core/base/events.h>
#include <ydb/core/kqp/common/simple/kqp_event_ids.h>

#include <util/datetime/base.h>

namespace NKikimr::NKqp {

struct TKqpWarmupEvents {
    enum EKqpWarmupEvents {
        EvWarmupComplete = EventSpaceBegin(TKikimrEvents::ES_KQP) + 900,
        EvWarmupRequest,
        EvWarmupResponse,
    };
};

struct TEvKqpWarmupComplete : public NActors::TEventLocal<TEvKqpWarmupComplete, TKqpWarmupEvents::EvWarmupComplete> {
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
    bool CompileCacheWarmupEnabled = true;
    ui32 MaxNodesToQuery = 5;
    ui32 MaxConcurrentCompilations = 5;
};

inline NActors::TActorId MakeKqpWarmupActorId(ui32 nodeId) {
    const char name[12] = "kqp_warmup";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

NActors::IActor* CreateKqpWarmupActor(
    const TKqpWarmupConfig& config,
    NActors::TActorId notifyActorId = {},
    const TString& database = {});

} // namespace NKikimr::NKqp
