#pragma once

#include "defs.h"

#include "queue_backpressure_common.h"

#include <ydb/library/actors/core/actor.h>

namespace NKikimr {

    inline TActorId MakeUniversalSchedulerActorId() {
        char name[12] = {'U', 'n', 'i', 'S', 'c', 'h', 'e', 'd', 'A', 'c', 't', 'r'};
        return TActorId(0, TStringBuf(name, sizeof(name)));
    }

    IActor *CreateUniversalSchedulerActor();

    bool RegisterActorInUniversalScheduler(const TActorId& actorId, TIntrusivePtr<NBackpressure::TFlowRecord> flowRecord,
        TActorSystem *actorSystem);

} // NKikimr
