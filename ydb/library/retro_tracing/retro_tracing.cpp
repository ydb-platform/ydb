#include "retro_tracing.h"
#include "span_circlebuf.h"

#include <util/system/spinlock.h>
#include <util/system/tls.h>

#include <vector>

namespace NRetro {

static thread_local std::atomic<TSpanCircleBuf*> TlsRetroSpanQueue = nullptr;

static std::vector<TSpanCircleBuf*>* RetroSpanQueues = nullptr;

void InitRetroTracing(ui32 numSlots) {
    static TSpinLock initializationLock;
    TGuard lock(initializationLock);
    if (!RetroSpanQueues) {
        RetroSpanQueues = new std::vector<TSpanCircleBuf*>();
    }

    if (!TlsRetroSpanQueue.load()) {
        TlsRetroSpanQueue.store(new TSpanCircleBuf(numSlots));
        RetroSpanQueues->push_back(TlsRetroSpanQueue.load());
    }
}

void WriteRetroSpanImpl(ERetroSpanType type, const ui8* data) {
    if (!TlsRetroSpanQueue.load()) {
        InitRetroTracing();
    }

    TlsRetroSpanQueue.load()->Write(type, data);
}

std::vector<TRetroSpan::TPtr> ReadSpansOfTrace(ui64 traceId) {
    if (!RetroSpanQueues) {
        InitRetroTracing();
    }
    
    std::vector<TRetroSpan::TPtr> res;
    for (TSpanCircleBuf* queue : *RetroSpanQueues) {
        Y_DEBUG_ABORT_UNLESS(queue);
        std::vector<TRetroSpan::TPtr> spans = queue->ReadSpansOfTrace(traceId);
        res.insert(res.end(), std::make_move_iterator(spans.begin()),
                std::make_move_iterator(spans.end()));
    }
    return res;
}

std::vector<std::shared_ptr<TSpanCircleBufStats>> GetBufferStatsSnapshots() {
    if (RetroSpanQueues) {
        std::vector<std::shared_ptr<TSpanCircleBufStats>> res;
        for (TSpanCircleBuf* queue : *RetroSpanQueues) {
            Y_DEBUG_ABORT_UNLESS(queue);
            res.push_back(queue->GetStatsSnapshot());
        }
        return res;
    } else {
        return {};
    }
}

} // namespace NRetro
