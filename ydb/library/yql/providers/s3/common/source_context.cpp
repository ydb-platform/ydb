#include "source_context.h"

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
    if (DownloadCount) {
        DownloadCount->Sub(GetSplitCount());
    }
    if (TaskDownloadCount) {
        TaskDownloadCount->Sub(GetSplitCount());
    }
    if (TaskChunkDownloadCount) {
        TaskChunkDownloadCount->Sub(GetChunkCount());
    }
}

void TSourceContext::IncChunkCount() {
    ChunkCount++;
    if (TaskChunkDownloadCount) {
        TaskChunkDownloadCount->Inc();
    }
}

void TSourceContext::DecChunkCount() {
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
    if ((Value.load() + delta / 2) >= Limit) {
        if (!paused) {
            {
                std::lock_guard<std::mutex> guard(Mutex);
                Producers.push_back(producer);
            }
            if (DownloadPaused) {
                DownloadPaused->Inc();
            }
            if (TaskDownloadPaused) {
                TaskDownloadPaused->Inc();
            }
            paused = true;
        }
    }
    return paused;
}

void TSourceContext::Sub(ui64 delta) {
    auto prev = Value.fetch_sub(delta);
    Y_ASSERT(prev >= delta);
    if (QueueDataSize) {
        QueueDataSize->Sub(delta);
    }
    if (TaskQueueDataSize) {
        TaskQueueDataSize->Sub(delta);
    }
    if (Value.load() * 4 < Limit * 3) { // l.eq.t 75%
        Notify();
    }
}

void TSourceContext::Notify() {
    std::size_t size = 0;
    {
        std::lock_guard<std::mutex> guard(Mutex);
        if (!Producers.empty()) {
            size = Producers.size();
            for (auto producer : Producers) {
                ActorSystem->Send(new NActors::IEventHandle(producer, NActors::TActorId{}, new TEvS3Provider::TEvContinue()));
            }
            Producers.clear();
        }
    }

    if (size) {
        if (DownloadPaused) {
            DownloadPaused->Sub(size);
        }
        if (TaskDownloadPaused) {
            TaskDownloadPaused->Sub(size);
        }
    }
}

void TSourceContext::IncSplitCount() {
    SplitCount++;
    if (DownloadCount) {
        DownloadCount->Inc();
    }
    if (TaskDownloadCount) {
        TaskDownloadCount->Inc();
    }
}

void TSourceContext::DecSplitCount() {
    SplitCount--;
    if (DownloadCount) {
        DownloadCount->Dec();
    }
    if (TaskDownloadCount) {
        TaskDownloadCount->Dec();
    }
}

void TSourceContext::UpdateProgress(ui64 deltaDownloadedBytes, ui64 deltaDecodedBytes, ui64 deltaDecodedRows) {
    DownloadedBytes += deltaDownloadedBytes;
    DecodedBytes += deltaDecodedBytes;
    DecodedRows += deltaDecodedRows;
}

} // namespace NYql::NDq
