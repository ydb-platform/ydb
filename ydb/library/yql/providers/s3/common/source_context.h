#pragma once

#include <memory>

#include <ydb/library/actors/core/actorsystem.h>

namespace NYql::NDq {

// common context for the Source (reads multiple splits)

struct TSourceContext {
    using TPtr = std::shared_ptr<TSourceContext>;

    TSourceContext(NActors::TActorId sourceId
        , ui64 limit
        , NActors::TActorSystem* actorSystem
        , NMonitoring::TDynamicCounters::TCounterPtr queueDataSize
        , NMonitoring::TDynamicCounters::TCounterPtr taskQueueDataSize
        , NMonitoring::TDynamicCounters::TCounterPtr downloadPaused
        , NMonitoring::TDynamicCounters::TCounterPtr taskDownloadPaused
        , NMonitoring::TDynamicCounters::TCounterPtr taskChunkDownloadCount
        , NMonitoring::THistogramPtr decodedChunkSizeHist)
        : SourceId(sourceId)
        , Limit(limit)
        , ActorSystem(actorSystem)
        , QueueDataSize(queueDataSize)
        , TaskQueueDataSize(taskQueueDataSize)
        , DownloadPaused(downloadPaused)
        , TaskDownloadPaused(taskDownloadPaused)
        , TaskChunkDownloadCount(taskChunkDownloadCount)
        , DecodedChunkSizeHist(decodedChunkSizeHist)
    {
    }

    ~TSourceContext();

    bool IsFull() const {
        return Value >= Limit;
    }

    double Ratio() const {
        return DownloadedBytes ? static_cast<double>(DecodedBytes) / DownloadedBytes : 1.0;
    }

    ui64 FairShare() {
        return CoroCount ? Limit / CoroCount : Limit;
    }

    void IncChunk();
    void DecChunk();
    bool Add(ui64 delta, NActors::TActorId producer, bool paused = false);
    void Sub(ui64 delta);
    void Notify();
    void UpdateProgress(ui64 deltaDownloadedBytes, ui64 deltaDecodedBytes, ui64 deltaDecodedRows);

    ui64 Value = 0;
    const NActors::TActorId SourceId;
    const ui64 Limit;
    NActors::TActorSystem* ActorSystem = nullptr;
    NMonitoring::TDynamicCounters::TCounterPtr QueueDataSize;
    NMonitoring::TDynamicCounters::TCounterPtr TaskQueueDataSize;
    NMonitoring::TDynamicCounters::TCounterPtr DownloadPaused;
    NMonitoring::TDynamicCounters::TCounterPtr TaskDownloadPaused;
    NMonitoring::TDynamicCounters::TCounterPtr TaskChunkDownloadCount;
    NMonitoring::THistogramPtr DecodedChunkSizeHist;
    ui64 CoroCount = 0;
    ui64 ChunkCount = 0;
    ui64 DownloadedBytes = 0;
    ui64 DecodedBytes = 0;
    ui64 DecodedRows = 0;
    std::vector<NActors::TActorId> Producers;
};

// per split context to pass params to load/decoding implementation

struct TSplitReadContext {
    TSourceContext::TPtr SourceContext;
};

} // namespace NYql::NDq
