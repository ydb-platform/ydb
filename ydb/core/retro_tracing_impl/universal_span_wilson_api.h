#pragma once

#include "named_span.h"
#include <ydb/library/actors/retro_tracing/universal_span.h>

namespace NKikimr {

template <class TRetroSpanType>
class TUniversalSpanWilsonApi : public NRetroTracing::TUniversalSpan<TRetroSpanType> {
public:
    TUniversalSpanWilsonApi() = default;

    TUniversalSpanWilsonApi(ui8 verbosity, NWilson::TTraceId parentId, const char* name,
            NWilson::TFlags flags = NWilson::EFlags::AUTO_END,
            NActors::TActorSystem* actorSystem = nullptr)
        : NRetroTracing::TUniversalSpan<TRetroSpanType>(verbosity, std::move(parentId),
                name, flags, actorSystem)
    {
        if constexpr (std::is_same_v<TRetroSpanType, TNamedSpan>) {
            if (TNamedSpan* span = this->GetRetroSpanPtr()) {
                span->SetName(name);
            }
        }
    }

    TUniversalSpanWilsonApi(NWilson::TSpan&& span)
        : NRetroTracing::TUniversalSpan<TRetroSpanType>(std::move(span))
    {}

    TUniversalSpanWilsonApi(TRetroSpanType&& span)
        : NRetroTracing::TUniversalSpan<TRetroSpanType>(std::move(span))
    {}

    TUniversalSpanWilsonApi(TUniversalSpanWilsonApi&&) = default;
    TUniversalSpanWilsonApi(const TUniversalSpanWilsonApi&) = delete;

    TUniversalSpanWilsonApi& operator=(TUniversalSpanWilsonApi&&) = default;
    TUniversalSpanWilsonApi& operator=(const TUniversalSpanWilsonApi&) = delete;

    TUniversalSpanWilsonApi& operator=(TRetroSpanType&& span) {
        this->Span.template emplace<TRetroSpanType>(std::forward<TRetroSpanType>(span));
        return *this;
    }

    TUniversalSpanWilsonApi& operator=(NWilson::TSpan&& span) {
        this->Span.template emplace<NWilson::TSpan>(std::move(span));
        return *this;
    }

    void Reset() {
        this->Span.template emplace<std::monostate>();
    }

    void ConstructRetroSpan(ui8 verbosity, NWilson::TTraceId parentId, const char* name,
            NWilson::TFlags flags = NWilson::EFlags::AUTO_END,
            NActors::TActorSystem* actorSystem = nullptr) {
        Y_ABORT_UNLESS(parentId.IsRetroTrace());
        this->Span.template emplace<TRetroSpanType>();
        std::get<TRetroSpanType>(this->Span).Initialize(verbosity, std::move(parentId), name, flags, actorSystem);
        if constexpr (std::is_same_v<TRetroSpanType, TNamedSpan>) {
            std::get<TRetroSpanType>(this->Span).SetName(name);
        }
    }

    void ConstructWilsonSpan(ui8 verbosity, NWilson::TTraceId parentId,
            std::variant<std::optional<TString>, const char*> name,
            NWilson::TFlags flags = NWilson::EFlags::AUTO_END,
            NActors::TActorSystem* actorSystem = nullptr) {
        Y_ABORT_UNLESS(parentId.IsWilsonTrace());
        this->Span.template emplace<NWilson::TSpan>(verbosity, std::move(parentId), std::move(name), flags, actorSystem);
    }

    explicit operator bool() const {
        bool res = false;
        std::visit(TOverloaded{
            [&](const NWilson::TSpan& span) -> void {
                res = (bool)span;
            },
            [&](const TRetroSpanType&) -> void {},
            [&](const std::monostate&) -> void {},
        }, this->Span);
        return res;
    }

    template <typename T>
    TUniversalSpanWilsonApi& Name(T&& name) {
        if constexpr (std::is_same_v<TRetroSpanType, TNamedSpan>) {
            std::visit(TOverloaded{
                [&](NWilson::TSpan& span) -> void {
                    span.Name(std::forward<T>(name));
                },
                [&](TRetroSpanType& span) -> void {
                    span.SetName(std::forward<T>(name));
                },
                [&](const std::monostate&) -> void {},
            }, this->Span);
        } else {
            std::visit(TOverloaded{
                [&](NWilson::TSpan& span) -> void {
                    span.Name(std::forward<T>(name));
                },
                [&](const TRetroSpanType&) -> void {},
                [&](const std::monostate&) -> void {},
            }, this->Span);
        }
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
