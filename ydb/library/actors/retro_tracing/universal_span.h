#pragma once

#include "retro_span.h"

#include <util/generic/overloaded.h>

#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

#include <variant>

namespace NKikimr {

template <class TRetroSpanType>
class TUniversalSpan {
public:
    enum class ESpanType : ui8 {
        Empty,
        Wilson,
        Retro,
    };

    TUniversalSpan() = default;

    TUniversalSpan(ui8 verbosity, const NWilson::TTraceId& parentId, const char* name,
            NWilson::TFlags flags = NWilson::EFlags::NONE,
            NActors::TActorSystem* actorSystem = nullptr) {
        if (parentId.IsRetroTrace()) {
            Span.template emplace<TRetroSpanType>(TRetroSpanType::Construct(verbosity, parentId, name,
                    flags, actorSystem));
        } else {
            Span.template emplace<NWilson::TSpan>(verbosity, NWilson::TTraceId(parentId), name, flags, actorSystem);
        }
    }

    NWilson::TSpan* GetWilsonSpanPtr() {
        NWilson::TSpan* res;
        std::visit(TOverloaded{
            [&](NWilson::TSpan& span) -> void { res = &span; },
            [&](const TRetroSpanType&) -> void { Y_ABORT("Attempted to get wilson span"
                    "from universal span initialized as retro"); },
            [&](const std::monostate&) -> void { Y_ABORT("Attempted to get wilson span"
                    "from uninitialized universal span"); },
        }, Span);
        return res;
    }

    const NWilson::TSpan* GetWilsonSpanPtr() const {
        const NWilson::TSpan* res;
        std::visit(TOverloaded{
            [&](const NWilson::TSpan& span) -> void { res = &span; },
            [&](const TRetroSpanType&) -> void { Y_ABORT("Attempted to get wilson span"
                    "from universal span initialized as retro"); },
            [&](const std::monostate&) -> void { Y_ABORT("Attempted to get wilson span"
                    "from uninitialized universal span"); },
        }, Span);
        return res;
    }

    TRetroSpanType* GetRetroSpanPtr() {
        TRetroSpanType* res;
        std::visit(TOverloaded{
            [&](const NWilson::TSpan&) -> void { Y_ABORT("Attempted to get retro span"
                    "from universal span initialized as wilson"); },
            [&](TRetroSpanType& span) -> void { res = &span; },
            [&](const std::monostate&) -> void { Y_ABORT("Attempted to get retro span"
                    "from uninitialized universal span"); },
        }, Span);
        return res;
    }

    const TRetroSpanType* GetRetroSpanPtr() const {
        TRetroSpanType* res;
        std::visit(TOverloaded{
            [&](const NWilson::TSpan&) -> void { Y_ABORT("Attempted to get retro span"
                    "from universal span initialized as wilson"); },
            [&](TRetroSpanType& span) -> void { res = &span; },
            [&](const std::monostate&) -> void { Y_ABORT("Attempted to get retro span"
                    "from uninitialized universal span"); },
        }, Span);
        return res;
    }

    ESpanType GetSpanType() {
        if (std::holds_alternative<std::monostate>(Span)) {
            return ESpanType::Empty;
        } else if (std::holds_alternative<NWilson::TSpan>(Span)) {
            return ESpanType::Wilson;
        } else {
            return ESpanType::Retro;
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

private:
    std::variant<std::monostate, NWilson::TSpan, TRetroSpanType> Span;
};

} // namespace NKikimr
