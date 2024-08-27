#include "tracer.h"
#include "private.h"

#include <yt/yt/library/tracing/jaeger/model.pb.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/tvm/service/tvm_service.h>

#include <yt/yt/core/rpc/grpc/channel.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/utilex/random.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <util/string/cast.h>
#include <util/string/reverse.h>

#include <util/system/getpid.h>
#include <util/system/env.h>
#include <util/system/byteorder.h>

#include <stack>

namespace NYT::NTracing {

using namespace NRpc;
using namespace NConcurrency;
using namespace NProfiling;
using namespace NYTree;
using namespace NAuth;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JaegerLogger;
static const auto& Profiler = TracingProfiler;

////////////////////////////////////////////////////////////////////////////////

static const TString ServiceTicketMetadataName = "x-ya-service-ticket";
static const TString TracingServiceAlias = "tracing";

////////////////////////////////////////////////////////////////////////////////

void TJaegerTracerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("collector_channel_config", &TThis::CollectorChannelConfig)
        .Optional();
    registrar.Parameter("max_request_size", &TThis::MaxRequestSize)
        .Default();
    registrar.Parameter("max_memory", &TThis::MaxMemory)
        .Default();
    registrar.Parameter("subsampling_rate", &TThis::SubsamplingRate)
        .Default();
    registrar.Parameter("flush_period", &TThis::FlushPeriod)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TJaegerTracerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("collector_channel_config", &TThis::CollectorChannelConfig)
        .Optional();

    // 10K nodes x 128 KB / 15s == 85mb/s
    registrar.Parameter("flush_period", &TThis::FlushPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("stop_timeout", &TThis::StopTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("queue_stall_timeout", &TThis::QueueStallTimeout)
        .Default(TDuration::Minutes(15));
    registrar.Parameter("max_request_size", &TThis::MaxRequestSize)
        .Default(128_KB)
        .LessThanOrEqual(4_MB);
    registrar.Parameter("max_batch_size", &TThis::MaxBatchSize)
        .Default(128);
    registrar.Parameter("max_memory", &TThis::MaxMemory)
        .Default(1_GB);
    registrar.Parameter("subsampling_rate", &TThis::SubsamplingRate)
        .Default();
    registrar.Parameter("reconnect_period", &TThis::ReconnectPeriod)
        .Default(TDuration::Minutes(15));
    registrar.Parameter("endpoint_channel_timeout", &TThis::EndpointChannelTimeout)
        .Default(TDuration::Hours(2));

    registrar.Parameter("service_name", &TThis::ServiceName)
        .Default();
    registrar.Parameter("process_tags", &TThis::ProcessTags)
        .Default();
    registrar.Parameter("enable_pid_tag", &TThis::EnablePidTag)
        .Default(false);

    registrar.Parameter("tvm_service", &TThis::TvmService)
        .Optional();

    registrar.Parameter("test_drop_spans", &TThis::TestDropSpans)
        .Default(false);
}

TJaegerTracerConfigPtr TJaegerTracerConfig::ApplyDynamic(const TJaegerTracerDynamicConfigPtr& dynamicConfig) const
{
    auto config = New<TJaegerTracerConfig>();
    config->CollectorChannelConfig = CollectorChannelConfig;
    if (dynamicConfig->CollectorChannelConfig) {
        config->CollectorChannelConfig = dynamicConfig->CollectorChannelConfig;
    }

    config->FlushPeriod = dynamicConfig->FlushPeriod.value_or(FlushPeriod);
    config->QueueStallTimeout = QueueStallTimeout;
    config->MaxRequestSize = dynamicConfig->MaxRequestSize.value_or(MaxRequestSize);
    config->MaxBatchSize = MaxBatchSize;
    config->MaxMemory = dynamicConfig->MaxMemory.value_or(MaxMemory);
    config->SubsamplingRate = SubsamplingRate;
    if (dynamicConfig->SubsamplingRate) {
        config->SubsamplingRate = dynamicConfig->SubsamplingRate;
    }

    config->ServiceName = ServiceName;
    config->ProcessTags = ProcessTags;
    config->EnablePidTag = EnablePidTag;
    config->TvmService = TvmService;
    config->TestDropSpans = TestDropSpans;

    config->Postprocess();
    return config;
}

bool TJaegerTracerConfig::IsEnabled() const
{
    return ServiceName && CollectorChannelConfig;
}

////////////////////////////////////////////////////////////////////////////////

class TJaegerCollectorProxy
    : public TProxyBase
{
public:
    DEFINE_RPC_PROXY(TJaegerCollectorProxy, jaeger.api_v2.CollectorService);

    DEFINE_RPC_PROXY_METHOD(NProto, PostSpans);
};

////////////////////////////////////////////////////////////////////////////////

namespace {

void ToProtoGuid(TString* proto, const TGuid& guid)
{
    *proto = TString{reinterpret_cast<const char*>(&guid.Parts32[0]), 16};
    ReverseInPlace(*proto);
}

void ToProtoUInt64(TString* proto, i64 i)
{
    i = SwapBytes64(i);
    *proto = TString{reinterpret_cast<char*>(&i), 8};
}

void ToProto(NProto::Span* proto, const TTraceContextPtr& traceContext)
{
    ToProtoGuid(proto->mutable_trace_id(), traceContext->GetTraceId());
    ToProtoUInt64(proto->mutable_span_id(), traceContext->GetSpanId());

    proto->set_operation_name(traceContext->GetSpanName());

    proto->mutable_start_time()->set_seconds(traceContext->GetStartTime().Seconds());
    proto->mutable_start_time()->set_nanos(traceContext->GetStartTime().NanoSecondsOfSecond());

    proto->mutable_duration()->set_seconds(traceContext->GetDuration().Seconds());
    proto->mutable_duration()->set_nanos(traceContext->GetDuration().NanoSecondsOfSecond());

    for (const auto& [name, value] : traceContext->GetTags()) {
        auto* protoTag = proto->add_tags();

        protoTag->set_key(name);
        protoTag->set_v_str(value);
    }

    for (const auto& logEntry : traceContext->GetLogEntries()) {
        auto* log = proto->add_logs();

        auto at = CpuInstantToInstant(logEntry.At);
        log->mutable_timestamp()->set_seconds(at.Seconds());
        log->mutable_timestamp()->set_nanos(at.NanoSecondsOfSecond());

        auto* message = log->add_fields();

        message->set_key("message");
        message->set_v_str(logEntry.Message);
    }

    int i = 0;
    for (const auto& traceId : traceContext->GetAsyncChildren()) {
        auto* tag = proto->add_tags();

        tag->set_key(Format("yt.async_trace_id.%v", i++));
        tag->set_v_str(ToString(traceId));
    }

    if (auto parentSpanId = traceContext->GetParentSpanId(); parentSpanId != InvalidSpanId) {
        auto* ref = proto->add_references();

        ToProtoGuid(ref->mutable_trace_id(), traceContext->GetTraceId());
        ToProtoUInt64(ref->mutable_span_id(), parentSpanId);
        ref->set_ref_type(NProto::CHILD_OF);
    }
}

template<typename TK, typename TV>
std::vector<TK> ExtractKeys(THashMap<TK, TV> const& inputMap) {
    std::vector<TK> retval;
    for (auto const& element : inputMap) {
        retval.push_back(element.first);
    }
    return retval;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TBatchInfo::TBatchInfo()
{ }

TBatchInfo::TBatchInfo(const TString& endpoint)
    : TracesDequeued_(Profiler().WithTag("endpoint", endpoint).Counter("/traces_dequeued"))
    , TracesDropped_(Profiler().WithTag("endpoint", endpoint).Counter("/traces_dropped"))
    , MemoryUsage_(Profiler().WithTag("endpoint", endpoint).Gauge("/memory_usage"))
    , TraceQueueSize_(Profiler().WithTag("endpoint", endpoint).Gauge("/queue_size"))
{ }

void TBatchInfo::PopFront()
{
    QueueMemory_ -= BatchQueue_[0].second.Size();
    QueueSize_ -= BatchQueue_[0].first;

    BatchQueue_.pop_front();

    MemoryUsage_.Update(QueueMemory_);
    TraceQueueSize_.Update(QueueSize_);
}

void TBatchInfo::EmplaceBack(int size, NYT::TSharedRef&& value)
{
    QueueMemory_ += value.size();
    QueueSize_ += size;

    BatchQueue_.emplace_back(size, value);

    MemoryUsage_.Update(QueueMemory_);
    TraceQueueSize_.Update(QueueSize_);
    TracesDequeued_.Increment(size);
}

std::pair<i64, i64> TBatchInfo::DropQueue(int batchCount)
{
    auto oldMemory = QueueMemory_;
    auto oldSize = QueueSize_;
    for (; batchCount > 0; batchCount--) {
        PopFront();
    }

    return {QueueMemory_ - oldMemory, QueueSize_ - oldSize};
}

void TBatchInfo::IncrementTracesDropped(i64 delta)
{
    TracesDropped_.Increment(delta);
}

std::tuple<std::vector<TSharedRef>, int, int> TBatchInfo::PeekQueue(const TJaegerTracerConfigPtr& config, std::optional<TSharedRef> processInfo)
{
    std::vector<TSharedRef> batches;
    if (processInfo) {
        batches.push_back(processInfo.value());
    }

    i64 memorySize = 0;
    int spanCount = 0;
    int batchCount = 0;

    for (; batchCount < std::ssize(BatchQueue_); batchCount++) {
        if (config && memorySize > config->MaxRequestSize) {
            break;
        }

        memorySize += BatchQueue_[batchCount].second.Size();
        spanCount += BatchQueue_[batchCount].first;
        batches.push_back(BatchQueue_[batchCount].second);
    }

    return std::tuple(batches, batchCount, spanCount);
}

TJaegerChannelManager::TJaegerChannelManager()
    : ReopenTime_(TInstant::Now())
{ }

TJaegerChannelManager::TJaegerChannelManager(
    const TJaegerTracerConfigPtr& config,
    const TString& endpoint,
    const ITvmServicePtr& tvmService)
    : TvmService_(tvmService)
    , Endpoint_(endpoint)
    , ReopenTime_(TInstant::Now() + config->ReconnectPeriod + RandomDuration(config->ReconnectPeriod))
    , RpcTimeout_(config->RpcTimeout)
    , PushedBytes_(Profiler().WithTag("endpoint", endpoint).Counter("/pushed_bytes"))
    , PushErrors_(Profiler().WithTag("endpoint", endpoint).Counter("/push_errors"))
    , PayloadSize_(Profiler().WithTag("endpoint", endpoint).Summary("/payload_size"))
    , PushDuration_(Profiler().WithTag("endpoint", endpoint).Timer("/push_duration"))
{
    auto channelEndpointConfig = CloneYsonStruct(config->CollectorChannelConfig);
    channelEndpointConfig->Address = endpoint;

    Channel_ = NGrpc::CreateGrpcChannel(channelEndpointConfig);
}

bool TJaegerChannelManager::Push(const std::vector<TSharedRef>& batches, int spanCount)
{
    try {
        TJaegerCollectorProxy proxy(Channel_);
        proxy.SetDefaultTimeout(RpcTimeout_);

        auto req = proxy.PostSpans();
        req->SetEnableLegacyRpcCodecs(false);
        req->set_batch(MergeRefsToString(batches));

        if (TvmService_) {
            auto* ticketExt = req->Header().MutableExtension(NRpc::NProto::TCustomMetadataExt::custom_metadata_ext);
            ticketExt->mutable_entries()->insert(
                {ServiceTicketMetadataName, TvmService_->GetServiceTicket(TracingServiceAlias)});
        }

        YT_LOG_DEBUG("Sending spans (SpanCount: %v, PayloadSize: %v, Endpoint: %v)",
            spanCount,
            req->batch().size(),
            Endpoint_);

        TEventTimerGuard timerGuard(PushDuration_);
        WaitFor(req->Invoke())
            .ThrowOnError();

        PushedBytes_.Increment(req->batch().size());
        PayloadSize_.Record(req->batch().size());
    } catch (const std::exception& ex) {
        PushErrors_.Increment();
        YT_LOG_ERROR(ex, "Failed to send spans (Endpoint: %v)", Endpoint_);
        return false;
    }

    return true;
}

bool TJaegerChannelManager::NeedsReopen(TInstant currentTime)
{
    if (Channel_ && currentTime >= ReopenTime_) {
        Channel_.Reset();
    }

    return !Channel_;
}

void TJaegerChannelManager::ForceReset(TInstant currentTime)
{
    currentTime = currentTime - TDuration::Minutes(1);
}

TInstant TJaegerChannelManager::GetReopenTime()
{
    return ReopenTime_;
}

TJaegerTracer::TJaegerTracer(
    const TJaegerTracerConfigPtr& config)
    : ActionQueue_(New<TActionQueue>("Jaeger"))
    , FlushExecutor_(New<TPeriodicExecutor>(
        ActionQueue_->GetInvoker(),
        BIND(&TJaegerTracer::Flush, MakeStrong(this)),
        config->FlushPeriod))
    , Config_(config)
    , TvmService_(config->TvmService ? CreateTvmService(config->TvmService) : nullptr)
{
    Profiler().AddFuncGauge("/enabled", MakeStrong(this), [this] {
        return Config_.Acquire()->IsEnabled();
    });

    FlushExecutor_->Start();
}

TFuture<void> TJaegerTracer::WaitFlush()
{
    return QueueEmptyPromise_.Load().ToFuture();
}

void TJaegerTracer::NotifyEmptyQueue()
{
    if (!TraceQueue_.IsEmpty() || TotalSize_ != 0) {
        return;
    }

    QueueEmptyPromise_.Exchange(NewPromise<void>()).Set();
}

void TJaegerTracer::Stop()
{
    YT_LOG_INFO("Stopping tracer");

    auto flushFuture = WaitFlush();
    FlushExecutor_->ScheduleOutOfBand();

    auto config = Config_.Acquire();
    Y_UNUSED(WaitFor(flushFuture.WithTimeout(config->StopTimeout)));

    YT_UNUSED_FUTURE(FlushExecutor_->Stop());
    ActionQueue_->Shutdown();

    YT_LOG_INFO("Tracer stopped");
}

void TJaegerTracer::Configure(const TJaegerTracerConfigPtr& config)
{
    Config_.Store(config);
    FlushExecutor_->SetPeriod(config->FlushPeriod);
}

void TJaegerTracer::Enqueue(TTraceContextPtr trace)
{
    TraceQueue_.Enqueue(std::move(trace));
}

void TJaegerTracer::DequeueAll(const TJaegerTracerConfigPtr& config)
{
    auto traces = TraceQueue_.DequeueAll();
    if (traces.empty() || !OpenChannelConfig_) {
        return;
    }

    THashMap<TString, NProto::Batch> batches;
    auto flushBatch = [&] (TString endpoint) {
        auto itBatch = batches.find(endpoint);
        if (itBatch == batches.end()) {
            return;
        }
        auto& batch = itBatch->second;
        if (batch.spans_size() == 0) {
            return;
        }

        auto itInfo = BatchInfo_.find(endpoint);
        if (itInfo == BatchInfo_.end()) {
            itInfo = BatchInfo_.insert({endpoint, TBatchInfo(endpoint)}).first;
        }
        auto& currentInfo = itInfo->second;

        if (TotalMemory_ > config->MaxMemory) {
            currentInfo.IncrementTracesDropped(batch.spans_size());
            batch.Clear();
            return;
        }

        auto tmpProto = SerializeProtoToRef(batch);

        auto sizeDelta = batch.spans_size();
        TotalMemory_ += tmpProto.size();

        currentInfo.EmplaceBack(sizeDelta, std::move(tmpProto));

        TotalSize_ += sizeDelta;

        batch.Clear();
    };

    for (const auto& trace : traces) {
        if (config->SubsamplingRate) {
            auto traceHash = THash<TGuid>()(trace->GetTraceId());

            if (((traceHash % 128) / 128.) > *config->SubsamplingRate) {
                continue;
            }
        }

        auto targetEndpoint = trace->GetTargetEndpoint();
        auto endpoint = targetEndpoint.value_or(OpenChannelConfig_->Address);

        ToProto(batches[endpoint].add_spans(), trace);

        if (batches[endpoint].spans_size() > config->MaxBatchSize) {
            flushBatch(endpoint);
        }
    }

    auto keys = ExtractKeys(batches);
    for (const auto& endpoint : keys) {
        flushBatch(endpoint);
    }
}

std::tuple<std::vector<TSharedRef>, int, int> TJaegerTracer::PeekQueue(const TJaegerTracerConfigPtr& config, const TString& endpoint)
{
    auto it = BatchInfo_.find(endpoint);
    if (it == BatchInfo_.end()) {
        return {{}, 0, 0};
    }

    std::optional<TSharedRef> processInfo;
    if (config && config->IsEnabled()) {
        processInfo = GetProcessInfo(config);
    }

    return it->second.PeekQueue(config, processInfo);
}

void TJaegerTracer::DropQueue(int batchCount, const TString& endpoint)
{
    auto it = BatchInfo_.find(endpoint);
    if (it == BatchInfo_.end()) {
        return;
    }

    auto& batchInfo = it->second;

    auto [memoryDelta, sizeDelta] = batchInfo.DropQueue(batchCount);

    TotalMemory_ += memoryDelta;
    TotalSize_ += sizeDelta;
}

void TJaegerTracer::DropFullQueue()
{
    auto keys = ExtractKeys(BatchInfo_);
    for (const auto& endpoint : keys) {
        while (true) {
            auto [batches, batchCount, spanCount] = PeekQueue(nullptr, endpoint);
            BatchInfo_[endpoint].IncrementTracesDropped(spanCount);
            DropQueue(batchCount, endpoint);

            if (batchCount == 0) {
                break;
            }
        }
    }
}

void TJaegerTracer::Flush()
{
    YT_LOG_DEBUG("Started span flush");

    auto config = Config_.Acquire();

    DequeueAll(config);

    if (TInstant::Now() - LastSuccessfulFlushTime_ > config->QueueStallTimeout) {
        YT_LOG_DEBUG("Queue stall timeout expired (QueueStallTimeout: %v)", config->QueueStallTimeout);
        DropFullQueue();
    }

    if (!config->IsEnabled()) {
        YT_LOG_DEBUG("Tracer is disabled");
        DropFullQueue();
        NotifyEmptyQueue();
        return;
    }

    auto flushStartTime = TInstant::Now();

    if (OpenChannelConfig_ != config->CollectorChannelConfig) {
        OpenChannelConfig_ = config->CollectorChannelConfig;
        for (auto& [endpoint, channel] : CollectorChannels_) {
            CollectorChannels_[endpoint].ForceReset(flushStartTime);
        }
    }

    std::stack<TString> toRemove;
    auto keys = ExtractKeys(BatchInfo_);

    if (keys.empty()) {
        YT_LOG_DEBUG("Span batch info is empty");
        LastSuccessfulFlushTime_ = flushStartTime;
        NotifyEmptyQueue();
        return;
    }

    for (const auto& endpoint : keys) {
        auto [batches, batchCount, spanCount] = PeekQueue(config, endpoint);
        if (batchCount <= 0) {
            if (!CollectorChannels_.contains(endpoint) || flushStartTime > CollectorChannels_[endpoint].GetReopenTime() + config->EndpointChannelTimeout) {
                toRemove.push(endpoint);
            }
            YT_LOG_DEBUG("Span queue is empty (Endpoint: %v)", endpoint);
            LastSuccessfulFlushTime_ = flushStartTime;
            continue;
        }

        if (config->TestDropSpans) {
            DropQueue(batchCount, endpoint);
            YT_LOG_DEBUG("Spans dropped in test (BatchCount: %v, SpanCount: %v, Endpoint: %v)",
                batchCount,
                spanCount,
                endpoint);
            continue;
        }

        auto it = CollectorChannels_.find(endpoint);
        if (it == CollectorChannels_.end()) {
            it = CollectorChannels_.emplace(endpoint, TJaegerChannelManager(config, endpoint, TvmService_)).first;
        }

        auto& channel = it->second;

        if (channel.NeedsReopen(flushStartTime)) {
            channel = TJaegerChannelManager(config, endpoint, TvmService_);
        }

        if (channel.Push(batches, spanCount)) {
            DropQueue(batchCount, endpoint);
            YT_LOG_DEBUG("Spans sent (Endpoint: %v)", endpoint);
            LastSuccessfulFlushTime_ = flushStartTime;
        }
    }

    while (!toRemove.empty()) {
        auto endpoint = toRemove.top();
        toRemove.pop();

        CollectorChannels_.erase(endpoint);
        BatchInfo_.erase(endpoint);
    }

    NotifyEmptyQueue();
}

TSharedRef TJaegerTracer::GetProcessInfo(const TJaegerTracerConfigPtr& config)
{
    NProto::Batch batch;
    auto* process = batch.mutable_process();
    process->set_service_name(*config->ServiceName);

    for (const auto& [key, value] : config->ProcessTags) {
        auto* tag = process->add_tags();
        tag->set_key(key);
        tag->set_v_str(value);
    }

    if (config->EnablePidTag) {
        auto* tag = process->add_tags();
        tag->set_key("pid");
        tag->set_v_str(::ToString(GetPID()));
    }

    return SerializeProtoToRef(batch);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
