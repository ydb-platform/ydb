#pragma once

#include "thread_context.h"
#include "worker_context.h"

#include <ydb/library/actors/actor_type/common.h>
#include <ydb/library/actors/actor_type/index_constructor.h>
#include <ydb/library/actors/util/local_process_key.h>

namespace NActors {


template <EInternalActorSystemActivity type>
class TInternalActorTypeGuard {
public:
    TInternalActorTypeGuard() {
        if (Enabled && TlsThreadContext) {
            NHPTimer::STime hpnow = GetCycleCountFast();
            NHPTimer::STime hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
            NextIndex = TlsThreadContext->ElapsingActorActivity.exchange(Index, std::memory_order_acq_rel);
            TlsThreadContext->WorkerCtx->AddElapsedCycles(NextIndex, hpnow - hpprev);
        }
    }

    TInternalActorTypeGuard(ui32 nextIndex)
        : NextIndex(nextIndex)
    {
        if (Enabled && TlsThreadContext) {
            NHPTimer::STime hpnow = GetCycleCountFast();
            NHPTimer::STime hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
            ui32 prevIndex = TlsThreadContext->ElapsingActorActivity.exchange(Index, std::memory_order_acq_rel);
            TlsThreadContext->WorkerCtx->AddElapsedCycles(prevIndex, hpnow - hpprev);
        }
    }

    TInternalActorTypeGuard(NHPTimer::STime hpnow) {
        if (Enabled && TlsThreadContext) {
            NHPTimer::STime hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
            NextIndex = TlsThreadContext->ElapsingActorActivity.exchange(Index, std::memory_order_acq_rel);
            TlsThreadContext->WorkerCtx->AddElapsedCycles(NextIndex, hpnow - hpprev);
        }
    }

    TInternalActorTypeGuard(NHPTimer::STime hpnow, ui32 nextIndex)
        : NextIndex(nextIndex)
    {
        if (Enabled && TlsThreadContext) {
            NHPTimer::STime hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
            ui32 prevIndex = TlsThreadContext->ElapsingActorActivity.exchange(Index, std::memory_order_acq_rel);
            TlsThreadContext->WorkerCtx->AddElapsedCycles(prevIndex, hpnow - hpprev);
        }
    }

    ~TInternalActorTypeGuard() {
        if (Enabled && TlsThreadContext) {
            NHPTimer::STime hpnow = GetCycleCountFast();
            NHPTimer::STime hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
            TlsThreadContext->ElapsingActorActivity.store(NextIndex, std::memory_order_release);
            TlsThreadContext->WorkerCtx->AddElapsedCycles(Index, hpnow - hpprev);
        }
    }


private:
    static constexpr bool Enabled = false;
    static ui32 Index;
    ui32 NextIndex = 0;
};

template <EInternalActorSystemActivity type>
ui32 TInternalActorTypeGuard<type>::Index = TEnumProcessKey<TActorActivityTag, EInternalActorSystemActivity>::GetIndex(type);

}