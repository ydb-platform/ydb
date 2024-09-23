#pragma once

#include <atomic>
#include <memory>
#include <mutex>

#include <ydb/library/yql/udfs/common/clickhouse/client/src/Formats/FormatSettings.h>
#include <ydb/library/yql/providers/s3/events/events.h>

#include <ydb/library/actors/core/actorsystem.h>

#include <ydb/core/kqp/common/kqp_tx.h>

#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_default_retry_policy.h>

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
        , NMonitoring::TDynamicCounters::TCounterPtr deferredQueueSize
        , const TString format
        , const TString compression
        , std::shared_ptr<arrow::Schema> schema
        , std::unordered_map<TStringBuf, NKikimr::NMiniKQL::TType*, THash<TStringBuf>> rowTypes
        , NDB::FormatSettings settings)
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
        , Format(format)
        , Compression(compression)
        , Schema(schema)
        , RowTypes(rowTypes)
        , Settings(settings)
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
    const TString Format;
    const TString Compression;
    std::shared_ptr<arrow::Schema> Schema;
    std::unordered_map<TStringBuf, NKikimr::NMiniKQL::TType*, THash<TStringBuf>> RowTypes;
    NDB::FormatSettings Settings;
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

// per split context to pass params to load/decoding implementation

struct TSplitReadContext {
    using TPtr = std::shared_ptr<TSplitReadContext>;

    TSplitReadContext(
        TSourceContext::TPtr sourceContext,
        IHTTPGateway::TPtr gateway,
        TString url,
        std::size_t splitOffset, std::size_t splitSize, std::size_t fileSize,
        std::size_t pathIndex,
        IHTTPGateway::THeaders headers,
        IHTTPGateway::TRetryPolicy::TPtr retryPolicy,
        TTxId txId, TString requestId)
        : SourceContext(sourceContext)
        , Gateway(gateway)
        , Url(url)
        , SplitOffset(splitOffset), SplitSize(splitSize), FileSize(fileSize)
        , PathIndex(pathIndex)
        , Headers(headers)
        , RetryPolicy(retryPolicy)
        , TxId(txId), RequestId(requestId)
    {

    }

    TSourceContext::TPtr SourceContext;

    const IHTTPGateway::TPtr Gateway;
    const TString Url;
    const std::size_t SplitOffset;
    const std::size_t SplitSize;
    const std::size_t FileSize;
    const std::size_t PathIndex;
    const IHTTPGateway::THeaders Headers;
    IHTTPGateway::TRetryPolicy::TPtr RetryPolicy;
    IHTTPGateway::TRetryPolicy::IRetryState::TPtr RetryState;
    IHTTPGateway::TCancelHook CancelHook;
    TMaybe<TDuration> NextRetryDelay;
    std::atomic_bool Cancelled = false;

    const TTxId TxId;
    const TString RequestId;

    const IHTTPGateway::TRetryPolicy::IRetryState::TPtr& GetRetryState() {
        if (!RetryState) {
            RetryState = RetryPolicy->CreateRetryState();
        }
        return RetryState;
    }

    void Cancel() {
        Cancelled.store(true);
        if (const auto cancelHook = std::move(CancelHook)) {
            CancelHook = {};
            cancelHook(TIssue("Request cancelled."));
        }
    }

    bool IsCancelled() {
        return Cancelled.load();
    }
};

} // namespace NYql::NDq
