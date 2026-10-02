#pragma once

#include "public.h"

#include <yt/yt/library/tracing/tracer.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/tvm/service/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/mpsc_stack.h>

#include <yt/yt/core/rpc/grpc/public.h>

#include <yt/yt/core/rpc/public.h>

#include <library/cpp/yt/threading/atomic_object.h>
#include <library/cpp/yt/threading/spin_lock.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

class TBatchInfo
{
public:
    TBatchInfo() = default;
    explicit TBatchInfo(const std::string& endpoint);

    void PopFront();
    void EmplaceBack(int size, NYT::TSharedRef&& value);
    std::pair<i64, i64> DropQueue(int spanCount);
    void IncrementTracesDropped(i64 delta);
    std::tuple<std::vector<TSharedRef>, int, int> PeekQueue(const TJaegerTracerConfigPtr& config, std::optional<TSharedRef> processInfo);

private:
    const NProfiling::TCounter TracesDequeued_;
    const NProfiling::TCounter TracesDropped_;
    const NProfiling::TGauge MemoryUsage_;
    const NProfiling::TGauge TraceQueueSize_;

    std::deque<std::pair<int, TSharedRef>> BatchQueue_;

    i64 QueueMemory_ = 0;
    i64 QueueSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TJaegerChannelManager
    : public TRefCounted
{
public:
    TJaegerChannelManager();
    TJaegerChannelManager(
        const TJaegerTracerConfigPtr& config,
        const std::string& endpoint,
        const NAuth::ITvmServicePtr& tvmService);

    bool Push(const std::vector<TSharedRef>& batches, int spanCount);
    bool NeedsReopen(TInstant currentTime);
    void ForceReset(TInstant currentTime);

    TInstant GetReopenTime();

private:
    const NAuth::ITvmServicePtr TvmService_;
    const std::string Endpoint_;

    const TInstant ReopenTime_;
    const TDuration RpcTimeout_;

    const NProfiling::TCounter PushedBytes_;
    const NProfiling::TCounter PushErrors_;
    const NProfiling::TSummary PayloadSize_;
    const NProfiling::TEventTimer PushDuration_;

    NRpc::IChannelPtr Channel_;
};

DEFINE_REFCOUNTED_TYPE(TJaegerChannelManager)

////////////////////////////////////////////////////////////////////////////////

class TJaegerTracer
    : public ITracer
{
public:
    explicit TJaegerTracer(TJaegerTracerConfigPtr config);

    TFuture<void> WaitFlush();

    void Configure(const TJaegerTracerConfigPtr& config);

    void Stop() override;

    void Enqueue(TTraceContextPtr trace) override;

private:
    const NConcurrency::TActionQueuePtr ActionQueue_;
    const NConcurrency::TPeriodicExecutorPtr FlushExecutor_;
    const NAuth::ITvmServicePtr TvmService_;

    TAtomicIntrusivePtr<TJaegerTracerConfig> Config_;

    TMpscStack<TTraceContextPtr> TraceQueue_;

    TInstant LastSuccessfulFlushTime_ = TInstant::Now();

    THashMap<std::string, TBatchInfo> BatchInfo_;
    i64 TotalMemory_ = 0;
    i64 TotalSize_ = 0;

    NThreading::TAtomicObject<TPromise<void>> QueueEmptyPromise_ = NewPromise<void>();

    THashMap<std::string, TJaegerChannelManagerPtr> CollectorChannels_;
    NRpc::NGrpc::TChannelConfigPtr OpenChannelConfig_;


    void DoFlush();
    void DequeueAll(const TJaegerTracerConfigPtr& config);
    void NotifyEmptyQueue();

    std::tuple<std::vector<TSharedRef>, int, int> PeekQueue(const TJaegerTracerConfigPtr& config, const std::string& endpoint);
    void DropQueue(int batchCount, const std::string& endpoint);
    void DropFullQueue();

    TSharedRef GetProcessInfo(const TJaegerTracerConfigPtr& config);
};

DEFINE_REFCOUNTED_TYPE(TJaegerTracer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
