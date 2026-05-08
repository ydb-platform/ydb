#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/open_telemetry/trace.h>

#include <opentelemetry/common/attribute_value.h>
#include <opentelemetry/context/runtime_context.h>
#include <opentelemetry/nostd/span.h>
#include <opentelemetry/trace/context.h>
#include <opentelemetry/trace/scope.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/span_context.h>
#include <opentelemetry/trace/tracer.h>
#include <opentelemetry/trace/tracer_provider.h>

#include <cstdio>

namespace NYdb::inline Dev::NTrace {

namespace {

namespace otel_trace = opentelemetry::trace;
namespace otel_nostd = opentelemetry::nostd;
namespace otel_common = opentelemetry::common;

otel_trace::SpanKind MapSpanKind(ESpanKind kind) {
    switch (kind) {
        case ESpanKind::INTERNAL: return otel_trace::SpanKind::kInternal;
        case ESpanKind::SERVER:   return otel_trace::SpanKind::kServer;
        case ESpanKind::CLIENT:   return otel_trace::SpanKind::kClient;
        case ESpanKind::PRODUCER: return otel_trace::SpanKind::kProducer;
        case ESpanKind::CONSUMER: return otel_trace::SpanKind::kConsumer;
    }
    return otel_trace::SpanKind::kInternal;
}

otel_trace::StatusCode MapSpanStatus(ESpanStatus status) {
    switch (status) {
        case ESpanStatus::Unset: return otel_trace::StatusCode::kUnset;
        case ESpanStatus::Ok:    return otel_trace::StatusCode::kOk;
        case ESpanStatus::Error: return otel_trace::StatusCode::kError;
    }
    return otel_trace::StatusCode::kUnset;
}

class TOtelScope : public IScope {
public:
    TOtelScope(otel_nostd::shared_ptr<otel_trace::Span> span)
        : Scope_(std::move(span))
    {}

private:
    otel_trace::Scope Scope_;
};

class TOtelSpan : public ISpan {
public:
    TOtelSpan(otel_nostd::shared_ptr<otel_trace::Span> span)
        : Span_(std::move(span))
    {}

    const otel_nostd::shared_ptr<otel_trace::Span>& RawSpan() const noexcept {
        return Span_;
    }

    void End() override {
        Span_->End();
    }

    void SetAttribute(std::string_view key, std::string_view value) override {
        Span_->SetAttribute(
            otel_nostd::string_view(key.data(), key.size()),
            otel_nostd::string_view(value.data(), value.size())
        );
    }

    void SetAttribute(std::string_view key, int64_t value) override {
        Span_->SetAttribute(otel_nostd::string_view(key.data(), key.size()), value);
    }

    void AddEvent(std::string_view name, TAttributes attributes) override {
        otel_nostd::string_view nameView(name.data(), name.size());
        if (attributes.size() == 0) {
            Span_->AddEvent(nameView);
        } else {
            std::vector<std::pair<otel_nostd::string_view, otel_common::AttributeValue>> attrs;
            attrs.reserve(attributes.size());
            for (const auto& [k, v] : attributes) {
                attrs.emplace_back(
                    otel_nostd::string_view(k.data(), k.size()),
                    otel_common::AttributeValue(otel_nostd::string_view(v.data(), v.size()))
                );
            }
            Span_->AddEvent(nameView, attrs);
        }
    }

    std::unique_ptr<IScope> Activate() override {
        return std::make_unique<TOtelScope>(Span_);
    }

    void SetStatus(ESpanStatus status, std::string_view description) override {
        Span_->SetStatus(
            MapSpanStatus(status),
            otel_nostd::string_view(description.data(), description.size())
        );
    }

private:
    otel_nostd::shared_ptr<otel_trace::Span> Span_;
};

std::string FormatTraceparent(const otel_trace::SpanContext& ctx) {
    if (!ctx.IsValid()) {
        return {};
    }
    constexpr int kTraceIdHexLen = 32;
    constexpr int kSpanIdHexLen = 16;
    char traceIdHex[kTraceIdHexLen];
    char spanIdHex[kSpanIdHexLen];
    ctx.trace_id().ToLowerBase16(otel_nostd::span<char, kTraceIdHexLen>(traceIdHex, kTraceIdHexLen));
    ctx.span_id().ToLowerBase16(otel_nostd::span<char, kSpanIdHexLen>(spanIdHex, kSpanIdHexLen));

    std::string out;
    out.reserve(2 + 1 + kTraceIdHexLen + 1 + kSpanIdHexLen + 1 + 2);
    out.append("00-");
    out.append(traceIdHex, kTraceIdHexLen);
    out.append("-");
    out.append(spanIdHex, kSpanIdHexLen);
    char flags[3];
    std::snprintf(flags, sizeof(flags), "%02x", static_cast<unsigned>(ctx.trace_flags().flags()));
    out.append("-");
    out.append(flags, 2);
    return out;
}

class TOtelTracer : public ITracer {
public:
    TOtelTracer(otel_nostd::shared_ptr<otel_trace::Tracer> tracer)
        : Tracer_(std::move(tracer))
    {}

    std::shared_ptr<ISpan> StartSpan(const std::string& name, ESpanKind kind) override {
        return StartSpan(name, kind, /*parent*/ nullptr);
    }

    std::shared_ptr<ISpan> StartSpan(const std::string& name, ESpanKind kind, ISpan* parent) override {
        otel_trace::StartSpanOptions options;
        options.kind = MapSpanKind(kind);
        if (auto* otelParent = dynamic_cast<TOtelSpan*>(parent)) {
            auto context = opentelemetry::context::RuntimeContext::GetCurrent();
            options.parent = otel_trace::SetSpan(context, otelParent->RawSpan());
        }
        return std::make_shared<TOtelSpan>(Tracer_->StartSpan(name, options));
    }

    std::string GetCurrentTraceparent() const override {
        auto ctx = opentelemetry::context::RuntimeContext::GetCurrent();
        auto span = otel_trace::GetSpan(ctx);
        if (!span) {
            return {};
        }
        return FormatTraceparent(span->GetContext());
    }

private:
    otel_nostd::shared_ptr<otel_trace::Tracer> Tracer_;
};

class TOtelTraceProvider : public ITraceProvider {
public:
    TOtelTraceProvider(otel_nostd::shared_ptr<otel_trace::TracerProvider> tracerProvider)
        : TracerProvider_(std::move(tracerProvider))
    {}

    std::shared_ptr<ITracer> GetTracer(const std::string& name) override {
        return std::make_shared<TOtelTracer>(TracerProvider_->GetTracer(name));
    }

private:
    otel_nostd::shared_ptr<otel_trace::TracerProvider> TracerProvider_;
};

} // namespace

std::shared_ptr<ITraceProvider> CreateOtelTraceProvider(
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider> tracerProvider)
{
    return std::make_shared<TOtelTraceProvider>(std::move(tracerProvider));
}

} // namespace NYdb::NTrace
