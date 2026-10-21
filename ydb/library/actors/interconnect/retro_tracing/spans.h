#pragma once

#include <ydb/library/actors/retro_tracing/span/retro_span.h>
#include <ydb/library/actors/retro_tracing/span/retro_span_namespace.h>
#include <ydb/library/actors/retro_tracing/span/typed_retro_span.h>
#include <ydb/library/actors/retro_tracing/span/universal_span.h>
#include <util/system/yassert.h>

namespace NActors {

struct TInterconnectRetroSpanType {
    enum E : ui32 {
        Packet = NRetroTracing::TSpanTypeNamespace::Begin(NRetroTracing::TSpanTypeNamespace::INTERCONNECT),
        DelayedEvent,
        End
    };

    static_assert(End < NRetroTracing::TSpanTypeNamespace::End(NRetroTracing::TSpanTypeNamespace::INTERCONNECT));
};

class TPacketSpan : public NRetroTracing::TTypedRetroSpan<TPacketSpan, TInterconnectRetroSpanType::Packet> {
public:
    using TUniversal = NRetroTracing::TUniversalSpan<TPacketSpan>;
};

class TDelayedEventSpan : public NRetroTracing::TTypedRetroSpan<TDelayedEventSpan,
        TInterconnectRetroSpanType::DelayedEvent> {
public:
    using TUniversal = NRetroTracing::TUniversalSpan<TDelayedEventSpan>;
};

NRetroTracing::TRetroSpan* DeserializeInterconnectRetroSpan(ui32 type, ui32 size, const void* data);

}  // namespace NActors
