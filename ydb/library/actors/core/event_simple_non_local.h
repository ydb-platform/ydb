#pragma once

#include "event.h"
#include "event_load.h"
#include <util/system/type_name.h>

namespace NActors {
    // Non-local event with empty serialization 
    template <typename TEv, ui32 TEventType>
    class TEventSimpleNonLocal: public TEventBase<TEv, TEventType> {
    public:
        TString ToStringHeader() const override {
            return TypeName<TEv>();
        }

        bool SerializeToArcadiaStream(TChunkSerializer* /*serializer*/) const override {
            return true;
        }

        bool IsSerializable() const override {
            return true;
        }

        static TEv* Load(const TEventSerializedData*) {
            return new TEv();
        }
    };
}
