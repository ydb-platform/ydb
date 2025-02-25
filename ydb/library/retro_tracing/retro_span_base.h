#pragma once

#include <concepts>
#include <memory>

#include <library/cpp/time_provider/monotonic.h>
#include <util/datetime/base.h>
#include <util/system/types.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/library/actors/wilson/wilson_span.h>

#include <ydb/library/retro_tracing/protos/retro.pb.h>

namespace NRetro {

enum ERetroSpanType : ui32 {
    None = 0,
    DSProxyRequest = 1,
    BackpressureInFlight,
    VDiskLogPut,
};

using TTraceId = ui64;
using TSpanId = ui64;

struct TFullSpanId {
    TFullSpanId(TTraceId traceId, TSpanId spanId);

    TTraceId TraceId;
    TSpanId SpanId;
};

TTraceId NewTraceId();
TSpanId NewSpanId();

void FillSpanId(const TFullSpanId& id, NRetroProto::TSpanId* proto);

TFullSpanId SpanIdToFullSpanId(const NRetroProto::TSpanId& proto);

class TRetroSpan {
public:
    using TPtr = std::unique_ptr<TRetroSpan>;

public:
    TRetroSpan(TInstant begin);
    TRetroSpan(TFullSpanId parentSpan, TInstant begin);
    TRetroSpan(const NRetroProto::TSpanId& parentSpan, TInstant begin);

    virtual ~TRetroSpan() = default;

    TFullSpanId GetId() const;
    std::optional<TSpanId> GetParentSpanId() const;
    virtual ERetroSpanType GetType() const = 0;
    virtual TString GetName() const;
    virtual std::vector<ui32> GetSubrequestNodeIds() const;
    TInstant GetStart() const;
    TInstant GetEnd() const;

    void End(TInstant ts);
    virtual void FillWilsonSpanAttributes(NWilson::TSpan* span) const;

    // TODO: priority and graceful degradation
    // ui32 GetPriority() const;

    static TPtr FromRawData(ui32 type, const void* data);

private:
    TFullSpanId Id;
    std::optional<TSpanId> ParentSpanId;
    TInstant StartTs;
    TInstant EndTs;

    bool Ended = false;
};

template <ERetroSpanType TypeId>
class TTypedRetroSpan : public TRetroSpan {
public:
    using TTyped = TTypedRetroSpan<TypeId>;

public:
    TTypedRetroSpan(TInstant start)
        : TRetroSpan(start)
    {}

    TTypedRetroSpan(TFullSpanId parentId, TInstant start)
        : TRetroSpan(parentId, start)
    {}

    ERetroSpanType GetType() const override {
        return TypeId;
    }

};

} // namespace NRetro
