#include "trace_context.h"
#include "private.h"
#include "config.h"
#include "allocation_tags.h"

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/singleton.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt_proto/yt/core/tracing/proto/tracing_ext.pb.h>

#include <yt/yt/library/tracing/tracer.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/yt/misc/tls.h>

#include <atomic>
#include <mutex>

namespace NYT::NTracing {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TracingLogger;

////////////////////////////////////////////////////////////////////////////////

struct TGlobalTracerStorage
{
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
    ITracerPtr Tracer;
};

// TODO(prime@): Switch constinit global variable, once gcc supports it.
static TGlobalTracerStorage* GlobalTracerStorage()
{
    return LeakySingleton<TGlobalTracerStorage>();
}

ITracerPtr GetGlobalTracer()
{
    auto tracerStorage = GlobalTracerStorage();
    auto guard = Guard(tracerStorage->Lock);
    return tracerStorage->Tracer;
}

void SetGlobalTracer(const ITracerPtr& tracer)
{
    ITracerPtr oldTracer;

    {
        auto tracerStorage = GlobalTracerStorage();
        auto guard = Guard(tracerStorage->Lock);
        oldTracer = tracerStorage->Tracer;
        tracerStorage->Tracer = tracer;
    }

    if (oldTracer) {
        oldTracer->Stop();
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TTracingConfigStorage
{
    TAtomicIntrusivePtr<TTracingTransportConfig> Config{New<TTracingTransportConfig>()};
};

static TTracingConfigStorage* GlobalTracingConfig()
{
    return LeakySingleton<TTracingConfigStorage>();
}

void SetTracingTransportConfig(TTracingTransportConfigPtr config)
{
    GlobalTracingConfig()->Config.Store(std::move(config));
}

TTracingTransportConfigPtr GetTracingTransportConfig()
{
    return GlobalTracingConfig()->Config.Acquire();
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

YT_THREAD_LOCAL(TTraceContext*) CurrentTraceContext;
YT_THREAD_LOCAL(TCpuInstant) TraceContextTimingCheckpoint;

TSpanId GenerateSpanId()
{
    return RandomNumber<ui64>(std::numeric_limits<ui64>::max() - 1) + 1;
}

void SetCurrentTraceContext(TTraceContext* context)
{
    CurrentTraceContext = context;
    std::atomic_signal_fence(std::memory_order::seq_cst);
}

TTraceContextPtr SwapTraceContext(TTraceContextPtr newContext)
{
    auto& propagatingStorage = GetCurrentPropagatingStorage();
    auto oldContext = propagatingStorage.Exchange<TTraceContextPtr>(newContext).value_or(nullptr);

    auto now = GetApproximateCpuInstant();
    // Invalid if no oldContext.
    auto delta = now - TraceContextTimingCheckpoint;

    if (oldContext && newContext) {
        YT_LOG_TRACE("Switching context (OldContext: %v, NewContext: %v, CpuTimeDelta: %v)",
            oldContext,
            newContext,
            NProfiling::CpuDurationToDuration(delta));
    } else if (oldContext) {
        YT_LOG_TRACE("Uninstalling context (Context: %v, CpuTimeDelta: %v)",
            oldContext,
            NProfiling::CpuDurationToDuration(delta));
    } else if (newContext) {
        YT_LOG_TRACE("Installing context (Context: %v)",
            newContext);
    }

    if (oldContext) {
        oldContext->IncrementElapsedCpuTime(delta);
    }

    SetCurrentTraceContext(newContext.Get());
    TraceContextTimingCheckpoint = now;

    return oldContext;
}

void OnContextSwitchOut()
{
    if (auto* context = TryGetCurrentTraceContext()) {
        auto now = GetApproximateCpuInstant();
        context->IncrementElapsedCpuTime(now - TraceContextTimingCheckpoint);
        SetCurrentTraceContext(nullptr);
        TraceContextTimingCheckpoint = 0;
    }
}

void OnContextSwitchIn()
{
    if (auto* context = TryGetTraceContextFromPropagatingStorage(GetCurrentPropagatingStorage())) {
        SetCurrentTraceContext(context);
        TraceContextTimingCheckpoint = GetApproximateCpuInstant();
    } else {
        SetCurrentTraceContext(nullptr);
        TraceContextTimingCheckpoint = 0;
    }
}

void OnPropagatingStorageSwitch(
    const TPropagatingStorage& oldStorage,
    const TPropagatingStorage& newStorage)
{
    TCpuInstant now = 0;

    if (auto* oldContext = TryGetCurrentTraceContext()) {
        YT_ASSERT(oldContext == TryGetTraceContextFromPropagatingStorage(oldStorage));
        YT_ASSERT(TraceContextTimingCheckpoint != 0);
        now = GetApproximateCpuInstant();
        oldContext->IncrementElapsedCpuTime(now - TraceContextTimingCheckpoint);
    }

    if (auto* newContext = TryGetTraceContextFromPropagatingStorage(newStorage)) {
        SetCurrentTraceContext(newContext);
        if (now == 0) {
            now = GetApproximateCpuInstant();
        }
        TraceContextTimingCheckpoint = now;
    } else {
        SetCurrentTraceContext(nullptr);
        TraceContextTimingCheckpoint = 0;
    }
}

void InitializeTraceContexts()
{
    static std::once_flag Initialized;
    std::call_once(
        Initialized,
        [] {
            InstallGlobalContextSwitchHandlers(
                OnContextSwitchOut,
                OnContextSwitchIn);
            InstallGlobalPropagatingStorageSwitchHandler(
                OnPropagatingStorageSwitch);
        });
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TSpanContext& context, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v:%08" PRIx64 ":%v",
        context.TraceId,
        context.SpanId,
        (context.Sampled ? 1u : 0) | (context.Debug ? 2u : 0));
}

TString ToString(const TSpanContext& context)
{
    return ToStringViaBuilder(context);
}

////////////////////////////////////////////////////////////////////////////////

TTraceContext::TTraceContext(
    TSpanContext parentSpanContext,
    TString spanName,
    TTraceContextPtr parentTraceContext)
    : TraceId_(parentSpanContext.TraceId)
    , SpanId_(NDetail::GenerateSpanId())
    , ParentSpanId_(parentSpanContext.SpanId)
    , Debug_(parentSpanContext.Debug)
    , State_(parentTraceContext
        ? parentTraceContext->State_.load()
        : (parentSpanContext.Sampled ? ETraceContextState::Sampled : ETraceContextState::Disabled))
    , Propagated_(true)
    , ParentContext_(std::move(parentTraceContext))
    , SpanName_(std::move(spanName))
    , RequestId_(ParentContext_ ? ParentContext_->GetRequestId() : TRequestId{})
    , TargetEndpoint_(ParentContext_ ? ParentContext_->GetTargetEndpoint() : std::nullopt)
    , LoggingTag_(ParentContext_ ? ParentContext_->GetLoggingTag() : TString{})
    , StartTime_(GetCpuInstant())
    , Baggage_(ParentContext_ ? ParentContext_->GetBaggage() : TYsonString{})
{
    NDetail::InitializeTraceContexts();
}

void TTraceContext::SetTargetEndpoint(const std::optional<TString>& targetEndpoint)
{
    TargetEndpoint_ = targetEndpoint;
}

void TTraceContext::SetRequestId(TRequestId requestId)
{
    RequestId_ = requestId;
}

void TTraceContext::SetLoggingTag(const TString& loggingTag)
{
    LoggingTag_ = loggingTag;
}

void TTraceContext::ClearAllocationTagsPtr() noexcept
{
    auto writerGuard = WriterGuard(AllocationTagsLock_);
    auto guard = Guard(AllocationTagsAsRefCountedLock_);
    AllocationTags_ = nullptr;
}

TAllocationTags::TTags TTraceContext::DoGetAllocationTags() const
{
    VERIFY_SPINLOCK_AFFINITY(AllocationTagsLock_);

    TAllocationTagsPtr tags;

    {
        // Local guard for copy RefCounted AllocationTags_.
        auto guard = Guard(AllocationTagsAsRefCountedLock_);
        tags = AllocationTags_;
    }

    if (!tags) {
        return {};
    }

    return tags->GetTags();
}

TAllocationTags::TTags TTraceContext::GetAllocationTags() const
{
    auto readerGuard = ReaderGuard(AllocationTagsLock_);
    return DoGetAllocationTags();
}

TAllocationTagsPtr TTraceContext::GetAllocationTagsPtr() const noexcept
{
    // Local guard for copy RefCounted AllocationTags_ for allocator callback CreateAllocationTagsData().
    auto guard = Guard(AllocationTagsAsRefCountedLock_);

    return AllocationTags_;
}

void TTraceContext::SetAllocationTagsPtr(TAllocationTagsPtr allocationTags) noexcept
{
    auto writerGuard = WriterGuard(AllocationTagsLock_);

    // Local guard for setting RefCounted AllocationTags_.
    auto guard = Guard(AllocationTagsAsRefCountedLock_);

    AllocationTags_ = std::move(allocationTags);
}

void TTraceContext::DoSetAllocationTags(TAllocationTags::TTags&& tags)
{
    VERIFY_SPINLOCK_AFFINITY(AllocationTagsLock_);

    TAllocationTagsPtr allocationTagsPtr;
    if (!tags.empty()) {
        // Allocation MUST be done BEFORE Guard(AllocationTagsAsRefCountedSpinlock_) to avoid deadlock with CreateAllocationTagsData().
        allocationTagsPtr = New<TAllocationTags>(std::move(tags));
    }

    auto guard = Guard(AllocationTagsAsRefCountedLock_);
    AllocationTags_ = std::move(allocationTagsPtr);
}

void TTraceContext::SetAllocationTags(TAllocationTags::TTags&& tags)
{
    auto writerGuard = WriterGuard(AllocationTagsLock_);

    return DoSetAllocationTags(std::move(tags));
}

void TTraceContext::SetRecorded()
{
    auto disabled = ETraceContextState::Disabled;
    State_.compare_exchange_strong(disabled, ETraceContextState::Recorded);
}

void TTraceContext::SetPropagated(bool value)
{
    Propagated_ = value;
}

TTraceContextPtr TTraceContext::CreateChild(
    TString spanName)
{
    auto child = New<TTraceContext>(
        GetSpanContext(),
        std::move(spanName),
        /*parentTraceContext*/ this);

    auto guard = Guard(Lock_);
    child->ProfilingTags_ = ProfilingTags_;
    child->TargetEndpoint_ = TargetEndpoint_;
    return child;
}

TSpanContext TTraceContext::GetSpanContext() const
{
    return TSpanContext{
        .TraceId = GetTraceId(),
        .SpanId = GetSpanId(),
        .Sampled = IsSampled(),
        .Debug = Debug_,
    };
}

TDuration TTraceContext::GetElapsedTime() const
{
    return CpuDurationToDuration(GetElapsedCpuTime());
}

void TTraceContext::SetSampled(bool value)
{
    if (!value) {
        State_ = ETraceContextState::Disabled;
    } else {
        State_ = ETraceContextState::Sampled;
    }
}

TInstant TTraceContext::GetStartTime() const
{
    return NProfiling::CpuInstantToInstant(StartTime_);
}

TDuration TTraceContext::GetDuration() const
{
    YT_ASSERT(Finished_.load());
    return NProfiling::CpuDurationToDuration(Duration_.load());
}

TTraceContext::TTagList TTraceContext::GetTags() const
{
    auto guard = Guard(Lock_);
    return Tags_;
}

TTraceContext::TLogList TTraceContext::GetLogEntries() const
{
    auto guard = Guard(Lock_);
    return Logs_;
}

TTraceContext::TAsyncChildrenList TTraceContext::GetAsyncChildren() const
{
    auto guard = Guard(Lock_);
    return AsyncChildren_;
}

TYsonString TTraceContext::GetBaggage() const
{
    auto guard = Guard(Lock_);
    return Baggage_;
}

void TTraceContext::SetBaggage(TYsonString baggage)
{
    auto guard = Guard(Lock_);
    Baggage_ = std::move(baggage);
}

IAttributeDictionaryPtr TTraceContext::UnpackBaggage() const
{
    auto baggage = GetBaggage();
    return baggage ? ConvertToAttributes(baggage) : nullptr;
}

NYTree::IAttributeDictionaryPtr TTraceContext::UnpackOrCreateBaggage() const
{
    auto baggage = GetBaggage();
    return baggage ? ConvertToAttributes(baggage) : CreateEphemeralAttributes();
}

void TTraceContext::PackBaggage(const IAttributeDictionaryPtr& baggage)
{
    SetBaggage(baggage ? ConvertToYsonString(baggage) : TYsonString{});
}

void TTraceContext::AddTag(const TString& tagKey, const TString& tagValue)
{
    if (!IsRecorded()) {
        return;
    }

    if (Finished_.load()) {
        return;
    }

    auto guard = Guard(Lock_);
    Tags_.emplace_back(tagKey, tagValue);
}

void TTraceContext::AddProfilingTag(const TString& name, const TString& value)
{
    auto guard = Guard(Lock_);
    ProfilingTags_.emplace_back(name, value);
}

void TTraceContext::AddProfilingTag(const TString& name, i64 value)
{
    auto guard = Guard(Lock_);
    ProfilingTags_.emplace_back(name, value);
}

std::vector<std::pair<TString, std::variant<TString, i64>>> TTraceContext::GetProfilingTags()
{
    auto guard = Guard(Lock_);
    return ProfilingTags_;
}

bool TTraceContext::AddAsyncChild(TTraceId traceId)
{
    if (!IsRecorded()) {
        return false;
    }

    if (Finished_.load()) {
        return false;
    }

    auto guard = Guard(Lock_);
    AsyncChildren_.push_back(traceId);
    return true;
}

void TTraceContext::AddErrorTag()
{
    if (!IsRecorded()) {
        return;
    }

    static const TString ErrorAnnotationName("error");
    static const TString ErrorAnnotationValue("true");
    AddTag(ErrorAnnotationName, ErrorAnnotationValue);
}

void TTraceContext::AddLogEntry(TCpuInstant at, TString message)
{
    if (!IsRecorded()) {
        return;
    }

    if (Finished_.load()) {
        return;
    }

    auto guard = Guard(Lock_);
    Logs_.push_back(TTraceLogEntry{at, std::move(message)});
}

bool TTraceContext::IsFinished()
{
    return Finished_.load();
}

bool TTraceContext::IsSampled() const
{
    auto traceContext = this;
    while (traceContext) {
        auto state = traceContext->State_.load(std::memory_order::relaxed);
        if (state == ETraceContextState::Sampled) {
            return true;
        } else if (state == ETraceContextState::Disabled) {
            return false;
        }

        traceContext = traceContext->ParentContext_.Get();
    }

    return false;
}

void TTraceContext::SetDuration()
{
    if (Duration_.load() == 0) {
        Duration_ = GetCpuInstant() - StartTime_;
    }
}

void TTraceContext::Finish()
{
    if (Finished_.exchange(true)) {
        return;
    }
    SetDuration();

    auto state = State_.load(std::memory_order::relaxed);
    if (state == ETraceContextState::Disabled) {
        return;
    } else if (state == ETraceContextState::Sampled) {
        if (auto tracer = GetGlobalTracer(); tracer) {
            tracer->Enqueue(MakeStrong(this));
        }
    } else if (state == ETraceContextState::Recorded) {
        if (!IsSampled()) {
            return;
        }

        if (auto tracer = GetGlobalTracer(); tracer) {
            auto traceContext = this;
            while (traceContext) {
                if (traceContext->State_.load() != ETraceContextState::Recorded) {
                    break;
                }

                if (traceContext->Finished_.load() && !traceContext->Submitted_.exchange(true)) {
                    traceContext->SetDuration();
                    tracer->Enqueue(MakeStrong(traceContext));
                }

                traceContext = traceContext->ParentContext_.Get();
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TTraceContext* context, TStringBuf /*spec*/)
{
    if (context) {
        builder->AppendFormat("%v %v",
            context->GetSpanName(),
            context->GetSpanContext());
    } else {
        builder->AppendString(TStringBuf("<null>"));
    }
}

TString ToString(const TTraceContext* context)
{
    return ToStringViaBuilder(context);
}

void FormatValue(TStringBuilderBase* builder, const TTraceContextPtr& context, TStringBuf spec)
{
    FormatValue(builder, context.Get(), spec);
}

TString ToString(const TTraceContextPtr& context)
{
    return ToStringViaBuilder(context);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TTracingExt* ext, const TTraceContextPtr& context)
{
    if (!context || !context->IsPropagated()) {
        ext->Clear();
        return;
    }

    ToProto(ext->mutable_trace_id(), context->GetTraceId());
    ext->set_span_id(context->GetSpanId());
    ext->set_sampled(context->IsSampled());
    ext->set_debug(context->IsDebug());

    if (auto endpoint = context->GetTargetEndpoint()){
        ext->set_target_endpoint(endpoint.value());
    }
    if (GetTracingTransportConfig()->SendBaggage) {
        if (auto baggage = context->GetBaggage()) {
            ext->set_baggage(baggage.ToString());
        }
    }
}

TTraceContextPtr TTraceContext::NewRoot(TString spanName, TTraceId traceId)
{
    return New<TTraceContext>(
        TSpanContext{
            .TraceId = traceId ? traceId : TTraceId::Create(),
            .SpanId = InvalidSpanId,
            .Sampled = false,
            .Debug = false,
        },
        std::move(spanName));
}

TTraceContextPtr TTraceContext::NewChildFromSpan(
    TSpanContext parentSpanContext,
    TString spanName,
    std::optional<TString> endpoint,
    TYsonString baggage)
{
    auto result = New<TTraceContext>(
        parentSpanContext,
        std::move(spanName));
    result->SetBaggage(std::move(baggage));
    result->SetTargetEndpoint(endpoint);
    return result;
}

TTraceContextPtr TTraceContext::NewChildFromRpc(
    const NProto::TTracingExt& ext,
    TString spanName,
    TRequestId requestId,
    bool forceTracing)
{
    auto traceId = FromProto<TTraceId>(ext.trace_id());
    if (!traceId) {
        if (!forceTracing) {
            return nullptr;
        }

        auto root = NewRoot(std::move(spanName));
        root->SetRequestId(requestId);
        root->SetRecorded();
        return root;
    }

    auto traceContext = New<TTraceContext>(
        TSpanContext{
            traceId,
            ext.span_id(),
            ext.sampled(),
            ext.debug()
        },
        std::move(spanName));
    traceContext->SetRequestId(requestId);
    if (ext.has_baggage()) {
        traceContext->SetBaggage(TYsonString(ext.baggage()));
    }
    if (ext.has_target_endpoint()) {
        traceContext->SetTargetEndpoint(ext.target_endpoint());
    }
    return traceContext;
}

void TTraceContext::IncrementElapsedCpuTime(NProfiling::TCpuDuration delta)
{
    for (auto* current = this; current; current = current->ParentContext_.Get()) {
        current->ElapsedCpuTime_ += delta;
    }
}

////////////////////////////////////////////////////////////////////////////////

void FlushCurrentTraceContextElapsedTime()
{
    auto* context = TryGetCurrentTraceContext();
    if (!context) {
        return;
    }

    auto now = GetApproximateCpuInstant();
    auto delta = std::max(now - NDetail::TraceContextTimingCheckpoint, static_cast<TCpuInstant>(0));
    YT_LOG_TRACE("Flushing context time (Context: %v, CpuTimeDelta: %v)",
        context,
        NProfiling::CpuDurationToDuration(delta));
    context->IncrementElapsedCpuTime(delta);
    NDetail::TraceContextTimingCheckpoint = now;
}

bool IsCurrentTraceContextRecorded()
{
    auto* context = TryGetCurrentTraceContext();
    return context && context->IsRecorded();
}

//! Do not rename, change the signature, or drop Y_NO_INLINE.
//! Used in devtools/gdb/yt_fibers_printer.py.
Y_NO_INLINE TTraceContext* TryGetTraceContextFromPropagatingStorage(const NConcurrency::TPropagatingStorage& storage)
{
    auto result = storage.Find<TTraceContextPtr>();
    return result ? result->Get() : nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

void* AcquireFiberTagStorage()
{
    auto* traceContext = NTracing::TryGetCurrentTraceContext();
    if (traceContext) {
        Ref(traceContext);
    }
    return reinterpret_cast<void*>(traceContext);
}

std::vector<std::pair<TString, std::variant<TString, i64>>> ReadFiberTags(void* storage)
{
    if (auto* traceContext = reinterpret_cast<NTracing::TTraceContext*>(storage)) {
        return traceContext->GetProfilingTags();
    } else {
        return {};
    }
}

void ReleaseFiberTagStorage(void* storage)
{
    if (storage) {
        Unref(reinterpret_cast<NTracing::TTraceContext*>(storage));
    }
}

TCpuInstant GetTraceContextTimingCheckpoint()
{
    return NTracing::NDetail::TraceContextTimingCheckpoint;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
