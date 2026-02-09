#include "named_span.h"

#include <ydb/library/actors/retro_tracing/universal_span.h>
#include "universal_span_wilson_api.h"

namespace NKikimr {

void TNamedSpan::SetName(const char* name) {
    NameSize = std::min(std::strlen(name), MaxNameSize);
    std::memcpy(reinterpret_cast<void*>(NameBuffer), reinterpret_cast<const void*>(name), NameSize);
}

TString TNamedSpan::GetName() const {
    return TString(NameBuffer, NameSize);
}

template<>
template<>
TUniversalSpanWilsonApi<TNamedSpan>& TUniversalSpanWilsonApi<TNamedSpan>::Name(const char* && name) {
    std::visit(TOverloaded{
        [&](NWilson::TSpan& span) -> void { span.Name(name); },
        [&](TNamedSpan& span) -> void { span.SetName(name); },
        [&](const std::monostate&) -> void {},
    }, Span);
    return *this;
}

} // namespace NKikimr

template<>
NRetroTracing::TUniversalSpan<NKikimr::TNamedSpan>::TUniversalSpan(ui8 verbosity, NWilson::TTraceId parentId,
        const char* name, NWilson::TFlags flags, NActors::TActorSystem* actorSystem) {
    if (!parentId) {
        // span is set to std::monostate
    } else if (parentId.IsRetroTrace()) {
        Span.template emplace<NKikimr::TNamedSpan>();
        std::get<NKikimr::TNamedSpan>(Span).Initialize(verbosity, std::move(parentId), name, flags, actorSystem);
        std::get<NKikimr::TNamedSpan>(Span).SetName(name);
    } else {
        Span.template emplace<NWilson::TSpan>(verbosity, std::move(parentId), name, flags, actorSystem);
    }
}
