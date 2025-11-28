#pragma once

#include <atomic>
#include <memory>
#include <mutex>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_default_retry_policy.h>
#include <ydb/library/yql/providers/s3/events/events.h>

namespace NYql::NDq {

// common context for the Source (reads multiple splits)

struct TSourceContext {
    using TPtr = std::shared_ptr<TSourceContext>;

    TSourceContext(NActors::TActorId sourceId
        , ui64 limit
        , NActors::TActorSystem* actorSystem
        , NMonitoring::TDynamicCounters::TCounterPtr queueDataSize
        , NMonitoring::TDynamicCounters::TCounterPtr taskQueueDataSize
        , NMonitoring::TDynamicCounters::TCounterPtr downloadCount
        , NMonitoring::TDynamicCounters::TCounterPtr downloadPaused
        , NMonitoring::TDynamicCounters::TCounterPtr taskDownloadCount
        , NMonitoring::TDynamicCounters::TCounterPtr taskDownloadPaused
        , NMonitoring::TDynamicCounters::TCounterPtr taskChunkDownloadCount
        , NMonitoring::THistogramPtr decodedChunkSizeHist
        , NMonitoring::TDynamicCounters::TCounterPtr httpInflightSize
        , NMonitoring::TDynamicCounters::TCounterPtr httpDataRps
        , NMonitoring::TDynamicCounters::TCounterPtr deferredQueueSize)
        : SourceId(sourceId)
        , Limit(limit)
        , ActorSystem(actorSystem)
        , QueueDataSize(queueDataSize)
        , TaskQueueDataSize(taskQueueDataSize)
        , DownloadCount(downloadCount)
        , DownloadPaused(downloadPaused)
        , TaskDownloadCount(taskDownloadCount)
        , TaskDownloadPaused(taskDownloadPaused)
        , TaskChunkDownloadCount(taskChunkDownloadCount)
        , DecodedChunkSizeHist(decodedChunkSizeHist)
        , HttpInflightSize(httpInflightSize)
        , HttpDataRps(httpDataRps)
        , DeferredQueueSize(deferredQueueSize)
    {
    }

    ~TSourceContext();

    bool IsFull() const {
        return Value.load() >= Limit;
    }

    double Ratio() const {
        auto downloadedBytes = DownloadedBytes.load();
        return downloadedBytes ? static_cast<double>(downloadedBytes) / DownloadedBytes.load() : 1.0;
    }

    ui64 FairShare() {
        auto splitCount = GetSplitCount();
        return splitCount ? Limit / splitCount : Limit;
    }

    ui64 GetValue() { return Value.load(); }
    ui32 GetChunkCount() { return ChunkCount.load(); }
    void IncChunkCount();
    void DecChunkCount();
    bool Add(ui64 delta, NActors::TActorId producer, bool paused = false);
    void Sub(ui64 delta);
    void Notify();
    void UpdateProgress(ui64 deltaDownloadedBytes, ui64 deltaDecodedBytes, ui64 deltaDecodedRows);
    ui32 GetSplitCount() { return SplitCount.load(); }
    void IncSplitCount();
    void DecSplitCount();
    ui64 GetDownloadedBytes() { return DownloadedBytes.load(); }

    const NActors::TActorId SourceId;
    const ui64 Limit;
    NActors::TActorSystem* ActorSystem;
    NMonitoring::TDynamicCounters::TCounterPtr QueueDataSize;
    NMonitoring::TDynamicCounters::TCounterPtr TaskQueueDataSize;
    NMonitoring::TDynamicCounters::TCounterPtr DownloadCount;
    NMonitoring::TDynamicCounters::TCounterPtr DownloadPaused;
    NMonitoring::TDynamicCounters::TCounterPtr TaskDownloadCount;
    NMonitoring::TDynamicCounters::TCounterPtr TaskDownloadPaused;
    NMonitoring::TDynamicCounters::TCounterPtr TaskChunkDownloadCount;
    NMonitoring::THistogramPtr DecodedChunkSizeHist;
    NMonitoring::TDynamicCounters::TCounterPtr HttpInflightSize;
    NMonitoring::TDynamicCounters::TCounterPtr HttpDataRps;
    NMonitoring::TDynamicCounters::TCounterPtr DeferredQueueSize;
private:
    std::atomic_uint64_t Value;
    std::mutex Mutex;
    std::vector<NActors::TActorId> Producers;
    std::atomic_uint32_t ChunkCount;
    std::atomic_uint32_t SplitCount;
    std::atomic_uint64_t DownloadedBytes;
    std::atomic_uint64_t DecodedBytes;
    std::atomic_uint64_t DecodedRows;
};


} // namespace NYql::NDq
