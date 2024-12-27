#pragma once

#include "allocation_tags.h"

#include <yt/yt/library/tracing/public.h>

#include <yt/yt/core/misc/guid.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/concurrency/public.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>
#include <library/cpp/yt/threading/spin_lock.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <atomic>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

struct TTracingAttributes
{
    TTraceId TraceId = InvalidTraceId;
    TSpanId SpanId = InvalidSpanId;
};

//! TSpanContext represents span identity propagated across the network.
//!
//! See https://opentracing.io/specification/
struct TSpanContext
{
    TTraceId TraceId = InvalidTraceId;
    TSpanId SpanId = InvalidSpanId;
    bool Sampled = false;
    bool Debug = false;
};

void FormatValue(TStringBuilderBase* builder, const TSpanContext& context, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

void SetGlobalTracer(const ITracerPtr& tracer);
ITracerPtr GetGlobalTracer();

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETraceContextState,
    (Disabled) // Used to propagate TraceId, RequestId and LoggingTag.
    (Recorded) // May be sampled later.
    (Sampled)  // Sampled and will be reported to tracer.
);

////////////////////////////////////////////////////////////////////////////////

//! Accumulates information associated with a single tracing span.
/*!
 *  TTraceContext contains 3 distinct pieces of logic.
 *
 *  1) TraceId, RequestId and LoggingTag are recorded inside trace context and
 *     passed to logger.
 *  2) ElapsedCpu time is tracked by fiber scheduler during context switch.
 *  3) Opentracing compatible information is recorded and later pushed to tracer.
 *
 *  TTraceContext objects within a single process form a tree.
 *
 *  By default, child objects inherit TraceId, RequestId and LoggingTag from the parent.
 *
 *  \note Thread affinity: any unless noted otherwise.
 */
class TTraceContext
    : public TRefCounted
{
public:
    //! Returns the flag indicating that this trace is finished (via call to #Finish).
    bool IsFinished() const;
    //! Finalizes and publishes the context (if sampling is enabled).
    /*!
     *  Safe to call multiple times from arbitrary threads; only the first call matters.
     */
    void Finish(
        std::optional<NProfiling::TCpuInstant> finishTime = {});

    //! Returns the flag indicating that this trace may be sent to tracer.
    /*!
     *  This flag should be used for fast-path optimization to skip trace annotation and child span creation.
     */
    bool IsRecorded() const;
    void SetRecorded();

    bool IsSampled() const;
    void SetSampled(bool value = true);

    //! Returns the flag indicating that trace is serialized to protobuf.
    /*!
     *  By default trace context is propagated.
     *  Not thread-safe.
     */
    bool IsPropagated() const;
    void SetPropagated(bool value = true);

    TSpanContext GetSpanContext() const;
    TTraceId GetTraceId() const;
    TSpanId GetSpanId() const;
    TSpanId GetParentSpanId() const;
    bool IsDebug() const;
    const TString& GetSpanName() const;

    //! Sets target endpoint.
    /*!
     *  Not thread-safe.
     */
    void SetTargetEndpoint(const std::optional<TString>& targetEndpoint);
    const std::optional<TString>& GetTargetEndpoint() const;

    //! Sets request id.
    /*!
     *  Not thread-safe.
     */
    void SetRequestId(TRequestId requestId);
    TRequestId GetRequestId() const;

    TAllocationTags GetAllocationTags() const;
    void SetAllocationTags(TAllocationTags&& tags);

    TAllocationTagListPtr GetAllocationTagList() const noexcept;
    void SetAllocationTagList(TAllocationTagListPtr list) noexcept;

    template <typename T>
    std::optional<T> FindAllocationTag(const TAllocationTagKey& key) const;
    template <typename T>
    std::optional<T> SetAllocationTag(const TAllocationTagKey& key, const T& value);
    void RemoveAllocationTag(const TAllocationTagKey& key);

    //! Sets logging tag.
    /*!
     *  Not thread-safe.
     */
    void SetLoggingTag(const TString& loggingTag);
    const TString& GetLoggingTag() const;

    TInstant GetStartTime() const;

    //! Returns the wall time from the context's construction to #Finish call.
    /*!
     *  Can only be called after #Finish is complete.
     */
    TDuration GetDuration() const;

    using TTagList = TCompactVector<std::pair<std::string, std::string>, 4>;
    TTagList GetTags() const;

    NYson::TYsonString GetBaggage() const;
    void SetBaggage(NYson::TYsonString baggage);
    NYTree::IAttributeDictionaryPtr UnpackBaggage() const;
    NYTree::IAttributeDictionaryPtr UnpackOrCreateBaggage() const;
    void PackBaggage(const NYTree::IAttributeDictionaryPtr& baggage);

    void AddTag(const std::string& tagKey, const std::string& tagValue);

    template <class T>
    void AddTag(const std::string& tagName, const T& tagValue);

    //! Adds error tag. Spans containing errors are highlighted in tracing UI.
    void AddErrorTag();

    struct TTraceLogEntry
    {
        NProfiling::TCpuInstant At;
        TString Message;
    };
    using TLogList = TCompactVector<TTraceLogEntry, 4>;
    TLogList GetLogEntries() const;
    void AddLogEntry(NProfiling::TCpuInstant at, TString message);

    using TAsyncChildrenList = TCompactVector<TTraceId, 4>;
    TAsyncChildrenList GetAsyncChildren() const;
    bool AddAsyncChild(TTraceId traceId);

    void IncrementElapsedCpuTime(NProfiling::TCpuDuration delta);
    NProfiling::TCpuDuration GetElapsedCpuTime() const;
    TDuration GetElapsedTime() const;

    static TTraceContextPtr NewRoot(const std::string& spanName, TTraceId traceId = {});

    static TTraceContextPtr NewChildFromRpc(
        const NProto::TTracingExt& ext,
        const std::string& spanName,
        TRequestId requestId = {},
        bool forceTracing = false);

    static TTraceContextPtr NewChildFromSpan(
        TSpanContext parentSpanContext,
        const std::string& spanName,
        std::optional<TString> endpoint = {},
        NYson::TYsonString baggage = NYson::TYsonString());

    TTraceContextPtr CreateChild(
        const std::string& spanName,
        std::optional<NProfiling::TCpuInstant> startTime = {});

    void AddProfilingTag(const std::string& name, const std::string& value);
    void AddProfilingTag(const std::string& name, i64 value);

    using TProfilingTagValue = std::variant<std::string, i64>;
    std::vector<std::pair<std::string, TProfilingTagValue>> GetProfilingTags();
    void SetProfilingTags(std::vector<std::pair<std::string, TProfilingTagValue>> profilingTags);

    friend void ToProto(
        NProto::TTracingExt* ext,
        const TTraceContextPtr& context,
        bool sendBaggage);

private:
    const TTraceId TraceId_;
    const TSpanId SpanId_;
    const TSpanId ParentSpanId_;
    // Right now, debug flag is just passed as-is. It is part of OpenTracing, but we do not interpret it in any way.
    const bool Debug_;

    std::atomic<ETraceContextState> State_;
    bool Propagated_ = true;

    const TTraceContextPtr ParentContext_;
    const TString SpanName_;
    TRequestId RequestId_;
    std::optional<TString> TargetEndpoint_;
    TString LoggingTag_;
    const NProfiling::TCpuInstant StartTime_;

    std::atomic<bool> Finished_ = false;
    std::atomic<bool> Submitted_ = false;
    std::atomic<NProfiling::TCpuInstant> FinishTime_ = 0;
    std::atomic<NProfiling::TCpuDuration> ElapsedCpuTime_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TTagList Tags_;
    TLogList Logs_;
    TAsyncChildrenList AsyncChildren_;
    NYson::TYsonString Baggage_;

    std::vector<std::pair<std::string, TProfilingTagValue>> ProfilingTags_;

    // Must NOT allocate memory while modifying AllocationTagList_ to avoid deadlock with allocator.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, AllocationTagsLock_);
    TAtomicIntrusivePtr<TAllocationTagList> AllocationTagList_;

    TTraceContext(
        TSpanContext parentSpanContext,
        const std::string& spanName,
        TTraceContextPtr parentTraceContext = nullptr,
        std::optional<NProfiling::TCpuInstant> startTime = {});
    DECLARE_NEW_FRIEND()

    void DoSetAllocationTags(TAllocationTags&& tags);

    void SubmitToTracer(const ITracerPtr& tracer);
};

DEFINE_REFCOUNTED_TYPE(TTraceContext)

void FormatValue(TStringBuilderBase* builder, const TTraceContextPtr& context, TStringBuf spec);
void FormatValue(TStringBuilderBase* builder, const TTraceContext* context, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

//! Returns the current trace context, if any is installed, or null if none.
TTraceContext* TryGetCurrentTraceContext();

//! Returns the current trace context. Fails if none.
TTraceContext* GetCurrentTraceContext();

//! Flushes the elapsed time of the current trace context (if any).
void FlushCurrentTraceContextElapsedTime();

//! Returns a trace context from #storage (null if there is none).
TTraceContext* TryGetTraceContextFromPropagatingStorage(const NConcurrency::TPropagatingStorage& storage);

//! Creates a new trace context. If the current trace context exists, it becomes the parent of the
//! created trace context.
TTraceContextPtr CreateTraceContextFromCurrent(const std::string& spanName);

////////////////////////////////////////////////////////////////////////////////

//! Installs the given trace into the current fiber implicit trace slot.
class TCurrentTraceContextGuard
{
public:
    explicit TCurrentTraceContextGuard(
        TTraceContextPtr traceContext,
        TSourceLocation location = YT_CURRENT_SOURCE_LOCATION);
    TCurrentTraceContextGuard(TCurrentTraceContextGuard&& other);
    ~TCurrentTraceContextGuard();

    bool IsActive() const;
    void Release();

    const TTraceContextPtr& GetOldTraceContext() const;

private:
    bool Active_;
    TTraceContextPtr OldTraceContext_;
};

////////////////////////////////////////////////////////////////////////////////

//! Installs null trace into the current fiber implicit trace slot.
class TNullTraceContextGuard
{
public:
    TNullTraceContextGuard(TSourceLocation location = YT_CURRENT_SOURCE_LOCATION);
    TNullTraceContextGuard(TNullTraceContextGuard&& other);
    ~TNullTraceContextGuard();

    bool IsActive() const;
    void Release();

    const TTraceContextPtr& GetOldTraceContext() const;

private:
    bool Active_;
    TTraceContextPtr OldTraceContext_;
};

////////////////////////////////////////////////////////////////////////////////

//! Invokes TTraceContext::Finish upon destruction.
class TTraceContextFinishGuard
{
public:
    explicit TTraceContextFinishGuard(TTraceContextPtr traceContext);
    ~TTraceContextFinishGuard();

    TTraceContextFinishGuard(const TTraceContextFinishGuard&) = delete;
    TTraceContextFinishGuard(TTraceContextFinishGuard&&) = default;

    TTraceContextFinishGuard& operator=(const TTraceContextFinishGuard&) = delete;
    TTraceContextFinishGuard& operator=(TTraceContextFinishGuard&&);

    void Release(
        std::optional<NProfiling::TCpuInstant> finishTime = {});
private:
    TTraceContextPtr TraceContext_;
};

////////////////////////////////////////////////////////////////////////////////

//! Installs the given trace into the current fiber implicit trace slot.
//! Finishes the trace context upon destruction.
class TTraceContextGuard
{
public:
    explicit TTraceContextGuard(TTraceContextPtr traceContext);
    TTraceContextGuard(TTraceContextGuard&& other) = default;

    void Release(
        std::optional<NProfiling::TCpuInstant> finishTime = {});

private:
    TCurrentTraceContextGuard TraceContextGuard_;
    TTraceContextFinishGuard FinishGuard_;
};

////////////////////////////////////////////////////////////////////////////////

//! Constructs a child trace context and installs it into the current fiber implicit trace slot.
//! Finishes the child trace context upon destruction.
class TChildTraceContextGuard
{
public:
    TChildTraceContextGuard(
        const TTraceContextPtr& traceContext,
        const std::string& spanName,
        std::optional<NProfiling::TCpuInstant> startTime = {});
    explicit TChildTraceContextGuard(
        const std::string& spanName,
        std::optional<NProfiling::TCpuInstant> startTime = {});
    TChildTraceContextGuard(TChildTraceContextGuard&& other) = default;

    void Finish(
        std::optional<NProfiling::TCpuInstant> finishTime = {});

private:
    TCurrentTraceContextGuard TraceContextGuard_;
    TTraceContextFinishGuard FinishGuard_;

    static bool IsRecorded(const TTraceContextPtr& traceContext);
};

////////////////////////////////////////////////////////////////////////////////

bool IsCurrentTraceContextRecorded();

template <class TFn>
void AnnotateTraceContext(TFn&& fn);

////////////////////////////////////////////////////////////////////////////////

class TTraceContextHandler
{
public:
    TTraceContextHandler();

    NTracing::TCurrentTraceContextGuard MakeTraceContextGuard() const;

    void UpdateTraceContext();

    std::optional<TTracingAttributes> GetTracingAttributes() const;

private:
    NTracing::TTraceContextPtr TraceContext_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

#define TRACE_CONTEXT_INL_H_
#include "trace_context-inl.h"
#undef TRACE_CONTEXT_INL_H_
