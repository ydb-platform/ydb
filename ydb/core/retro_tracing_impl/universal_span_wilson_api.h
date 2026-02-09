#pragma once

#include <ydb/library/actors/retro_tracing/universal_span.h>

namespace NKikimr {

template <class TRetroSpanType>
class TUniversalSpanWilsonApi : public NRetroTracing::TUniversalSpan<TRetroSpanType> {
public:
    TUniversalSpanWilsonApi() = default;

    TUniversalSpanWilsonApi(ui8 verbosity, NWilson::TTraceId parentId, const char* name,
            NWilson::TFlags flags = NWilson::EFlags::NONE,
            NActors::TActorSystem* actorSystem = nullptr)
        : NRetroTracing::TUniversalSpan<TRetroSpanType>(verbosity, std::move(parentId),
                name, flags, actorSystem)
    {}

    TUniversalSpanWilsonApi(NWilson::TSpan&& span)
        : NRetroTracing::TUniversalSpan<TRetroSpanType>(std::move(span))
    {}

    TUniversalSpanWilsonApi(TRetroSpanType&& span)
        : NRetroTracing::TUniversalSpan<TRetroSpanType>(std::move(span))
    {}

    TUniversalSpanWilsonApi(TUniversalSpanWilsonApi&& span) = default;
    TUniversalSpanWilsonApi(const TUniversalSpanWilsonApi& span) = delete;

    template<typename T>
    TUniversalSpanWilsonApi& Name(T&& name) {
        std::visit(TOverloaded{
            [&](NWilson::TSpan& span) -> void {
                span.Name(std::move(name));
            },
            [&](const TRetroSpanType&) -> void {},
            [&](const std::monostate&) -> void {},
        }, this->Span);
        return *this;
    }

    template<typename T, typename T1>
    TUniversalSpanWilsonApi& Attribute(T&& name, T1&& value) {
        std::visit(TOverloaded{
            [&](NWilson::TSpan& span) -> void {
                span.Attribute(std::move(name), std::move(value));
            },
            [&](const TRetroSpanType&) -> void {},
            [&](const std::monostate&) -> void {},
        }, this->Span);
        return *this;
    }

    template<typename T, typename T1 =
            std::initializer_list<std::pair<const char*, NWilson::TAttributeValue>>>
    TUniversalSpanWilsonApi& Event(T&& name, T1&& attributes) {
        std::visit(TOverloaded{
            [&](NWilson::TSpan& span) -> void {
                span.Event(std::move(name), std::move(attributes));
            },
            [&](const TRetroSpanType&) -> void {},
            [&](const std::monostate&) -> void {},
        }, this->Span);
        return *this;
    }

    TUniversalSpanWilsonApi& Link(const NWilson::TTraceId& traceId) {
        std::visit(TOverloaded{
            [&](NWilson::TSpan& span) -> void { span.Link(traceId); },
            [&](const TRetroSpanType&) -> void {},
            [&](const std::monostate&) -> void {},
        }, this->Span);
        return *this;
    }

    template<typename T = std::initializer_list<std::pair<TString, NWilson::TAttributeValue>>>
    TUniversalSpanWilsonApi& Link(const NWilson::TTraceId& traceId, T&& attributes) {
        std::visit(TOverloaded{
            [&](NWilson::TSpan& span) -> void { span.Link(traceId, std::move(attributes)); },
            [&](const TRetroSpanType&) -> void {},
            [&](const std::monostate&) -> void {},
        }, this->Span);
        return *this;
    }
};

} // namespace NKikimr
