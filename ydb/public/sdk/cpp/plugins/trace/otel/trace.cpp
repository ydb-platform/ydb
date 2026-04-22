#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/open_telemetry/trace.h>

#include <opentelemetry/common/attribute_value.h>
#include <opentelemetry/trace/tracer.h>
#include <opentelemetry/trace/tracer_provider.h>

namespace NYdb::inline Dev::NTrace {

namespace {

using namespace opentelemetry;

trace::SpanKind MapSpanKind(ESpanKind kind) {
    switch (kind) {
        case ESpanKind::INTERNAL: return trace::SpanKind::kInternal;
        case ESpanKind::SERVER:   return trace::SpanKind::kServer;
        case ESpanKind::CLIENT:   return trace::SpanKind::kClient;
        case ESpanKind::PRODUCER: return trace::SpanKind::kProducer;
        case ESpanKind::CONSUMER: return trace::SpanKind::kConsumer;
    }
    return trace::SpanKind::kInternal;
}

class TOtelSpan : public ISpan {
public:
    TOtelSpan(nostd::shared_ptr<trace::Span> span)
        : Span_(std::move(span))
    {}

    void End() override {
        Span_->End();
    }

    void SetAttribute(const std::string& key, const std::string& value) override {
        Span_->SetAttribute(key, value);
    }

    void SetAttribute(const std::string& key, int64_t value) override {
        Span_->SetAttribute(key, value);
    }

    void AddEvent(const std::string& name, const std::map<std::string, std::string>& attributes) override {
        if (attributes.empty()) {
            Span_->AddEvent(name);
        } else {
            std::vector<std::pair<nostd::string_view, common::AttributeValue>> attrs;
            attrs.reserve(attributes.size());
            for (const auto& [k, v] : attributes) {
                attrs.emplace_back(nostd::string_view(k), common::AttributeValue(nostd::string_view(v)));
            }
            Span_->AddEvent(name, attrs);
        }
    }

private:
    nostd::shared_ptr<trace::Span> Span_;
};

class TOtelTracer : public ITracer {
public:
    TOtelTracer(nostd::shared_ptr<trace::Tracer> tracer)
        : Tracer_(std::move(tracer))
    {}

    std::shared_ptr<ISpan> StartSpan(const std::string& name, ESpanKind kind) override {
        trace::StartSpanOptions options;
        options.kind = MapSpanKind(kind);
        return std::make_shared<TOtelSpan>(Tracer_->StartSpan(name, options));
    }

private:
    nostd::shared_ptr<trace::Tracer> Tracer_;
};

class TOtelTraceProvider : public ITraceProvider {
public:
    TOtelTraceProvider(nostd::shared_ptr<trace::TracerProvider> tracerProvider)
        : TracerProvider_(std::move(tracerProvider))
    {}

    std::shared_ptr<ITracer> GetTracer(const std::string& name) override {
        return std::make_shared<TOtelTracer>(TracerProvider_->GetTracer(name));
    }

private:
    nostd::shared_ptr<trace::TracerProvider> TracerProvider_;
};

} // namespace

std::shared_ptr<ITraceProvider> CreateOtelTraceProvider(
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider> tracerProvider)
{
    return std::make_shared<TOtelTraceProvider>(std::move(tracerProvider));
}

} // namespace NYdb::NTrace
