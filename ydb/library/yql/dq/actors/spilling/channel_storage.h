#pragma once

#include "spilling_counters.h"

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/runtime/dq_channel_storage.h>
#include <ydb/library/actors/core/actor.h>
#include <yql/essentials/minikql/computation/mkql_spiller.h>

namespace NActors {
    class TActorSystem;
};

namespace NYql::NDq {

IDqChannelStorage::TPtr CreateDqChannelStorage(TTxId txId, ui64 channelId,
    TWakeUpCallback wakeUpCallback,
    TErrorCallback errorCallback,
    TIntrusivePtr<TSpillingTaskCounters> spillingTaskCounters,
    NActors::TActorSystem* actorSystem);

// Create channel storage with shared spiller
IDqChannelStorage::TPtr CreateDqChannelStorageWithSharedSpiller(ui64 channelId,
    NKikimr::NMiniKQL::ISpiller::TPtr sharedSpiller,
    TIntrusivePtr<TSpillingTaskCounters> spillingTaskCounters);

} // namespace NYql::NDq
