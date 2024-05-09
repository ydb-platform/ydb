#include "source_context.h"

#include <ydb/library/yql/providers/s3/events/events.h>

namespace NYql::NDq {

TSourceContext::~TSourceContext() {
    Notify();
    if (Value) {
        if (QueueDataSize) {
            QueueDataSize->Sub(Value);
        }
        if (TaskQueueDataSize) {
            TaskQueueDataSize->Sub(Value);
        }
        Value = 0;
    }
}

void TSourceContext::IncChunk() {
    ChunkCount++;
    if (TaskChunkDownloadCount) {
        TaskChunkDownloadCount->Inc();
    }
}

void TSourceContext::DecChunk() {
    ChunkCount--;
    if (TaskChunkDownloadCount) {
        TaskChunkDownloadCount->Dec();
    }
}

bool TSourceContext::Add(ui64 delta, NActors::TActorId producer, bool paused) {
    if (DecodedChunkSizeHist) {
        DecodedChunkSizeHist->Collect(delta);
    }
    Value += delta;
    if (QueueDataSize) {
        QueueDataSize->Add(delta);
    }
    if (TaskQueueDataSize) {
        TaskQueueDataSize->Add(delta);
    }
    if ((Value + delta / 2) >= Limit) {
        if (!paused) {
            if (DownloadPaused) {
                DownloadPaused->Inc();
            }
            if (TaskDownloadPaused) {
                TaskDownloadPaused->Inc();
            }
            Producers.push_back(producer);
            paused = true;
        }
    }
    return paused;
}

void TSourceContext::Sub(ui64 delta) {
    Y_ASSERT(Value >= delta);
    Value -= delta;
    if (QueueDataSize) {
        QueueDataSize->Sub(delta);
    }
    if (TaskQueueDataSize) {
        TaskQueueDataSize->Sub(delta);
    }
    if (Value * 4 < Limit * 3) { // l.eq.t 75%
        Notify();
    }
}

void TSourceContext::Notify() {
    if (!Producers.empty()) {
        if (DownloadPaused) {
            DownloadPaused->Sub(Producers.size());
        }
        if (TaskDownloadPaused) {
            TaskDownloadPaused->Sub(Producers.size());
        }
        for (auto producer : Producers) {
            ActorSystem->Send(new NActors::IEventHandle(producer, NActors::TActorId{}, new TEvS3Provider::TEvContinue()));
        }
        Producers.clear();
    }
}

void TSourceContext::UpdateProgress(ui64 deltaDownloadedBytes, ui64 deltaDecodedBytes, ui64 deltaDecodedRows) {
    DownloadedBytes += deltaDownloadedBytes;
    DecodedBytes += deltaDecodedBytes;
    DecodedRows += deltaDecodedRows;
}

} // namespace NYql::NDq
