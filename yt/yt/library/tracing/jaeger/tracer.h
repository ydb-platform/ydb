#pragma once

#include "public.h"

#include <yt/yt/library/tracing/tracer.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/tvm/service/config.h>
#include <yt/yt/library/tvm/service/public.h>

#include <yt/yt/core/misc/mpsc_stack.h>
#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/rpc/grpc/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

class TJaegerTracerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    NRpc::NGrpc::TChannelConfigPtr CollectorChannelConfig;

    std::optional<i64> MaxRequestSize;

    std::optional<i64> MaxMemory;

    std::optional<double> SubsamplingRate;

    std::optional<TDuration> FlushPeriod;

    REGISTER_YSON_STRUCT(TJaegerTracerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJaegerTracerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TJaegerTracerConfig
    : public NYTree::TYsonStruct
{
public:
    NRpc::NGrpc::TChannelConfigPtr CollectorChannelConfig;

    TDuration FlushPeriod;

    TDuration StopTimeout;

    TDuration RpcTimeout;

    TDuration EndpointChannelTimeout;

    TDuration QueueStallTimeout;

    TDuration ReconnectPeriod;

    i64 MaxRequestSize;

    i64 MaxBatchSize;

    i64 MaxMemory;

    std::optional<double> SubsamplingRate;

    // ServiceName is required by jaeger. When ServiceName is missing, tracer is disabled.
    std::optional<TString> ServiceName;

    THashMap<TString, TString> ProcessTags;

    bool EnablePidTag;

    NAuth::TTvmServiceConfigPtr TvmService;

    // Does not send spans to a collector, but just drops them instead. Logs batch and span count.
    bool TestDropSpans;

    TJaegerTracerConfigPtr ApplyDynamic(const TJaegerTracerDynamicConfigPtr& dynamicConfig) const;

    bool IsEnabled() const;

    REGISTER_YSON_STRUCT(TJaegerTracerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJaegerTracerConfig)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJaegerTracer)

class TBatchInfo
{
public:
    TBatchInfo();
    TBatchInfo(const TString& endpoint);

    void PopFront();
    void EmplaceBack(int size, NYT::TSharedRef&& value);
    std::pair<i64, i64> DropQueue(int spanCount);
    void IncrementTracesDropped(i64 delta);
    std::tuple<std::vector<TSharedRef>, int, int> PeekQueue(const TJaegerTracerConfigPtr& config, std::optional<TSharedRef> processInfo);

private:
    std::deque<std::pair<int, TSharedRef>> BatchQueue_;

    i64 QueueMemory_ = 0;
    i64 QueueSize_ = 0;

    NProfiling::TCounter TracesDequeued_;
    NProfiling::TCounter TracesDropped_;
    NProfiling::TGauge MemoryUsage_;
    NProfiling::TGauge TraceQueueSize_;
};

class TJaegerChannelManager
{
public:
    TJaegerChannelManager();
    TJaegerChannelManager(
        const TJaegerTracerConfigPtr& config,
        const TString& endpoint,
        const NAuth::ITvmServicePtr& tvmService);

    bool Push(const std::vector<TSharedRef>& batches, int spanCount);
    bool NeedsReopen(TInstant currentTime);
    void ForceReset(TInstant currentTime);

    TInstant GetReopenTime();

private:
    NRpc::IChannelPtr Channel_;
    NAuth::ITvmServicePtr TvmService_;

    TString Endpoint_;

    TInstant ReopenTime_;
    TDuration RpcTimeout_;

    NProfiling::TCounter PushedBytes_;
    NProfiling::TCounter PushErrors_;
    NProfiling::TSummary PayloadSize_;
    NProfiling::TEventTimer PushDuration_;
};

class TJaegerTracer
    : public ITracer
{
public:
    TJaegerTracer(const TJaegerTracerConfigPtr& config);

    TFuture<void> WaitFlush();

    void Configure(const TJaegerTracerConfigPtr& config);

    void Stop() override;

    void Enqueue(TTraceContextPtr trace) override;

private:
    const NConcurrency::TActionQueuePtr ActionQueue_;
    const NConcurrency::TPeriodicExecutorPtr FlushExecutor_;

    TAtomicIntrusivePtr<TJaegerTracerConfig> Config_;

    TMpscStack<TTraceContextPtr> TraceQueue_;

    TInstant LastSuccessfulFlushTime_ = TInstant::Now();

    THashMap<TString, TBatchInfo> BatchInfo_;
    i64 TotalMemory_ = 0;
    i64 TotalSize_ = 0;

    TAtomicObject<TPromise<void>> QueueEmptyPromise_ = NewPromise<void>();

    THashMap<TString, TJaegerChannelManager> CollectorChannels_;
    NRpc::NGrpc::TChannelConfigPtr OpenChannelConfig_;

    NAuth::ITvmServicePtr TvmService_;

    void Flush();
    void DequeueAll(const TJaegerTracerConfigPtr& config);
    void NotifyEmptyQueue();

    std::tuple<std::vector<TSharedRef>, int, int> PeekQueue(const TJaegerTracerConfigPtr& config, const TString& endpoint);
    void DropQueue(int batchCount, const TString& endpoint);
    void DropFullQueue();

    TSharedRef GetProcessInfo(const TJaegerTracerConfigPtr& config);
};

DEFINE_REFCOUNTED_TYPE(TJaegerTracer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
