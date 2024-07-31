#pragma once

#include "event.h"
#include "scheduler_cookie.h"
#include "event_load.h"
#include <util/system/type_name.h>

namespace NActors {
    template <typename TEv, ui32 TEventType>
    class TEventLocal: public TEventBase<TEv, TEventType> {
    public:
        TString ToStringHeader() const override {
            return TypeName<TEv>();
        }

        bool SerializeToArcadiaStream(TChunkSerializer* /*serializer*/) const override {
            Y_ABORT("Serialization of local event %s type %" PRIu32, TypeName<TEv>().data(), TEventType);
        }

        bool IsSerializable() const override {
            return false;
        }

        static IEventBase* Load(TEventSerializedData*) {
            Y_ABORT("Loading of local event %s type %" PRIu32, TypeName<TEv>().data(), TEventType);
        }
    };

}
