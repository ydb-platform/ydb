#pragma once

#include "retro_span.h"

#include <util/datetime/base.h>
#include <util/generic/overloaded.h>

#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

#include <variant>

namespace NRetroTracing {

enum class EUniversalSpanType : ui8 {
    Empty,
    Wilson,
    Retro,
};

template <class TRetroSpanType>
class TUniversalSpan {
public:
    TUniversalSpan() = default;

    TUniversalSpan(ui8 verbosity, NWilson::TTraceId parentId, const char* name,
            NWilson::TFlags flags = NWilson::EFlags::NONE,
            NActors::TActorSystem* actorSystem = nullptr) {
        if (!parentId) {
            // span is set to std::monostate
        } else if (parentId.IsRetroTrace()) {
            Span.template emplace<TRetroSpanType>();
            std::get<TRetroSpanType>(Span).Initialize(verbosity, std::move(parentId), name, flags, actorSystem);
        } else {
            Span.template emplace<NWilson::TSpan>(verbosity, std::move(parentId), name, flags, actorSystem);
        }
    }

    TUniversalSpan(NWilson::TSpan&& span)
        : Span(std::move(span))
    {}

    TUniversalSpan(TRetroSpanType&& span)
        : Span(std::move(span))
    {}

    TUniversalSpan(TUniversalSpan&&) = default;
    TUniversalSpan(const TUniversalSpan&) = delete;

    TUniversalSpan& operator=(TUniversalSpan&&) = default;
    TUniversalSpan& operator=(const TUniversalSpan&) = delete;

    NWilson::TSpan* GetWilsonSpanPtr() {
        NWilson::TSpan* res;
        std::visit(TOverloaded{
            [&](NWilson::TSpan& span) -> void { res = &span; },
            [&](const TRetroSpanType&) -> void { res = nullptr; },
            [&](const std::monostate&) -> void { res = nullptr; },
        }, Span);
        return res;
    }

    const NWilson::TSpan* GetWilsonSpanPtr() const {
        const NWilson::TSpan* res;
        std::visit(TOverloaded{
            [&](const NWilson::TSpan& span) -> void { res = &span; },
            [&](const TRetroSpanType&) -> void { res = nullptr; },
            [&](const std::monostate&) -> void { res = nullptr; },
        }, Span);
        return res;
    }

    TRetroSpanType* GetRetroSpanPtr() {
        TRetroSpanType* res;
        std::visit(TOverloaded{
            [&](const NWilson::TSpan&) -> void { res = nullptr; },
            [&](TRetroSpanType& span) -> void { res = &span; },
            [&](const std::monostate&) -> void { res = nullptr; },
        }, Span);
        return res;
    }

    const TRetroSpanType* GetRetroSpanPtr() const {
        const TRetroSpanType* res;
        std::visit(TOverloaded{
            [&](const NWilson::TSpan&) -> void { res = nullptr; },
            [&](TRetroSpanType& span) -> void { res = &span; },
            [&](const std::monostate&) -> void { res = nullptr; },
        }, Span);
        return res;
    }

    EUniversalSpanType GetSpanType() const {
        if (std::holds_alternative<std::monostate>(Span)) {
            return EUniversalSpanType::Empty;
        } else if (std::holds_alternative<NWilson::TSpan>(Span)) {
            return EUniversalSpanType::Wilson;
        } else {
            return EUniversalSpanType::Retro;
        }
    }

    NWilson::TTraceId GetTraceId() const {
        NWilson::TTraceId res{};
        std::visit(TOverloaded{
            [&](const NWilson::TSpan& span) -> void { res = span.GetTraceId(); },
            [&](const TRetroSpanType& span) -> void { res = span.GetTraceId(); },
            [&](const std::monostate&) -> void {},
        }, Span);
        return res;
    }

    TString GetName() const {
        TString res;
        std::visit(TOverloaded{
            [&](const NWilson::TSpan& span) -> void { res = span.GetName(); },
            [&](const TRetroSpanType& span) -> void { res = span.GetName(); },
            [&](const std::monostate&) -> void {},
        }, Span);
        return res;
    }

    TUniversalSpan& EnableAutoEnd() {
        std::visit(TOverloaded{
            [&](NWilson::TSpan& span) -> void { span.EnableAutoEnd(); },
            [&](TRetroSpanType& span) -> void { span.EnableAutoEnd(); },
            [&](const std::monostate&) -> void {},
        }, Span);
        return *this;
    }

    void EndOk() {
        std::visit(TOverloaded{
            [&](NWilson::TSpan& span) -> void { span.EndOk(); },
            [&](TRetroSpanType& span) -> void { span.EndOk(); },
            [&](const std::monostate&) -> void {},
        }, Span);
    }

    template<typename T>
    void EndError(T&& error) {
        std::visit(TOverloaded{
            [&](NWilson::TSpan& span) -> void { span.EndError(std::move(error)); },
            [&](TRetroSpanType& span) -> void { span.EndError(); },
            [&](const std::monostate&) -> void {},
        }, Span);
    }

protected:
    std::variant<std::monostate, NWilson::TSpan, TRetroSpanType> Span;
};

} // namespace NKikimr
