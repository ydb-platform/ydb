#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include "retro_span.h"
#include "span_buffer.h"

namespace NRetroTracing {

template <ui32 Id, typename T>
class TTypedRetroSpan : public TRetroSpan {
protected:
    static constexpr ui32 TypeId = Id;

public:
    TTypedRetroSpan()
        : TRetroSpan(Id, sizeof(T))
    {}

    // Initializes retro-span from args from default wilson args
    void Initialize(ui8 verbosity, const NWilson::TTraceId& parentId, const char* name,
            NWilson::TFlags flags = NWilson::EFlags::NONE,
            NActors::TActorSystem* actorSystem = nullptr) {
        Y_UNUSED(verbosity);
        Y_UNUSED(name);
        Y_UNUSED(actorSystem);
        AttachToTrace(parentId);
        Flags = flags;
    }

    virtual TString GetName() const override {
        return typeid(T).name();
    }
};

} // namespace NRetroTracing
