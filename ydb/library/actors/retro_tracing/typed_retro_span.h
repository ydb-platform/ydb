#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include "retro_span.h"

namespace NRetroTracing {

template <ui32 Id, typename T>
class TTypedRetroSpan : public TRetroSpan {
protected:
    static constexpr ui32 TypeId = Id;

public:
    TTypedRetroSpan()
        : TRetroSpan(Id, sizeof(T))
    {
        StartTs = TInstant::Now();
    }
    
    ~TTypedRetroSpan() {
        if (Flags & NWilson::EFlags::AUTO_END) {
            End();
        }
    }

    // Constructs retro-span from args for wilson span
    static T Construct(ui8 verbosity, const NWilson::TTraceId& parentId, const char* name,
            NWilson::TFlags flags = NWilson::EFlags::NONE,
            NActors::TActorSystem* actorSystem = nullptr) {
        Y_UNUSED(verbosity);
        Y_UNUSED(name);
        Y_UNUSED(actorSystem);

        T res;
        res.AttachToTrace(parentId);
        res.Flags = flags;
        return res;
    }

    virtual TString GetName() const override {
        return typeid(T).name();
    }

    void End() {
        if (!IsEnded()) {
            EndTs = TInstant::Now();
        }
    }
};

} // namespace NRetroTracing
