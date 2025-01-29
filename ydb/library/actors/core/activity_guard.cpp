#include "activity_guard.h"

#include "thread_context.h"
#include "worker_context.h"

namespace NActors {

    void ChangeActivity(NHPTimer::STime hpnow, ui32 &prevIndex, ui32 &index) {
        if (TlsThreadContext) {
            NHPTimer::STime hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
            prevIndex = TlsThreadContext->ActivityContext.ElapsingActorActivity.exchange(index, std::memory_order_acq_rel);
            if (prevIndex != SleepActivity) {
                TlsThreadContext->WorkerCtx->AddElapsedCycles(prevIndex, hpnow - hpprev);
            } else {
                TlsThreadContext->WorkerCtx->AddParkedCycles(hpnow - hpprev);
            }
        }
    }

}
