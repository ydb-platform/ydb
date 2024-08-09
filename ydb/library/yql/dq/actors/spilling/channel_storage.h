#pragma once

#include "spilling_counters.h"

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/runtime/dq_channel_storage.h>
#include <ydb/library/actors/core/actor.h>

namespace NActors {
    class TActorSystem;
};

namespace NYql::NDq {

IDqChannelStorage::TPtr CreateDqChannelStorage(TTxId txId, ui64 channelId, IDqChannelStorage::TWakeUpCallback wakeUpCb,
    TIntrusivePtr<TSpillingTaskCounters> spillingTaskCounters, NActors::TActorSystem* actorSystem);

} // namespace NYql::NDq
