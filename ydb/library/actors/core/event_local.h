#pragma once

#include "event.h"
#include "scheduler_cookie.h"
#include "event_load.h"
#include <util/system/type_name.h>

namespace NActors {
    namespace NDetail {
        [[noreturn]] void AbortLocalEventSerialization(const std::type_info& eventTypeInfo, ui32 eventType);
    }

    template <typename TEv, ui32 TEventType>
    class TEventLocal: public TEventBase<TEv, TEventType> {
    public:
        TString ToStringHeader() const override {
            return TypeName<TEv>();
        }

        bool SerializeToArcadiaStream(TChunkSerializer* /*serializer*/) const override {
            NDetail::AbortLocalEventSerialization(typeid(TEv), TEventType);
        }

        bool IsSerializable() const override {
            return false;
        }
    };

}
