#pragma once

#include "thread_context.h"
#include "worker_context.h"

#include <ydb/library/actors/actor_type/common.h>
#include <ydb/library/actors/actor_type/index_constructor.h>
#include <ydb/library/actors/util/local_process_key.h>

namespace NActors {


template <EInternalActorSystemActivity ActivityType, bool IsMainActivity=true>
class TInternalActorTypeGuard {
public:
    TInternalActorTypeGuard() {
        if (Allowed && TlsThreadContext) {
            NHPTimer::STime hpnow = GetCycleCountFast();
            NHPTimer::STime hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
            NextIndex = TlsThreadContext->ElapsingActorActivity.exchange(Index, std::memory_order_acq_rel);
            TlsThreadContext->WorkerCtx->AddElapsedCycles(NextIndex, hpnow - hpprev);
        }
    }

    TInternalActorTypeGuard(ui32 nextIndex)
        : NextIndex(nextIndex)
    {
        if (Allowed && TlsThreadContext) {
            NHPTimer::STime hpnow = GetCycleCountFast();
            NHPTimer::STime hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
            ui32 prevIndex = TlsThreadContext->ElapsingActorActivity.exchange(Index, std::memory_order_acq_rel);
            TlsThreadContext->WorkerCtx->AddElapsedCycles(prevIndex, hpnow - hpprev);
        }
    }

    TInternalActorTypeGuard(NHPTimer::STime hpnow) {
        if (Allowed && TlsThreadContext) {
            NHPTimer::STime hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
            NextIndex = TlsThreadContext->ElapsingActorActivity.exchange(Index, std::memory_order_acq_rel);
            TlsThreadContext->WorkerCtx->AddElapsedCycles(NextIndex, hpnow - hpprev);
        }
    }

    TInternalActorTypeGuard(NHPTimer::STime hpnow, ui32 nextIndex)
        : NextIndex(nextIndex)
    {
        if (Allowed && TlsThreadContext) {
            NHPTimer::STime hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
            ui32 prevIndex = TlsThreadContext->ElapsingActorActivity.exchange(Index, std::memory_order_acq_rel);
            TlsThreadContext->WorkerCtx->AddElapsedCycles(prevIndex, hpnow - hpprev);
        }
    }

    ~TInternalActorTypeGuard() {
        if (Allowed && TlsThreadContext) {
            NHPTimer::STime hpnow = GetCycleCountFast();
            NHPTimer::STime hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
            TlsThreadContext->ElapsingActorActivity.store(NextIndex, std::memory_order_release);
            TlsThreadContext->WorkerCtx->AddElapsedCycles(Index, hpnow - hpprev);
        }
    }


private:
    static constexpr bool ExtraActivitiesIsAllowed = false;
    static constexpr bool Allowed = ExtraActivitiesIsAllowed || IsMainActivity;
    static ui32 Index;
    ui32 NextIndex = 0;
};

template <EInternalActorSystemActivity ActivityType, bool IsMainActivity>
ui32 TInternalActorTypeGuard<ActivityType, IsMainActivity>::Index = TEnumProcessKey<TActorActivityTag, EInternalActorSystemActivity>::GetIndex(ActivityType);

}