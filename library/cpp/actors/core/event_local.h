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

    template <typename TEv, ui32 TEventType>
    class TEventScheduler: public TEventLocal<TEv, TEventType> {
    public:
        TSchedulerCookieHolder Cookie;

        TEventScheduler(ISchedulerCookie* cookie)
            : Cookie(cookie)
        {
        }
    };

    template <ui32 TEventType>
    class TEventSchedulerEv: public TEventScheduler<TEventSchedulerEv<TEventType>, TEventType> {
    public:
        TEventSchedulerEv(ISchedulerCookie* cookie)
            : TEventScheduler<TEventSchedulerEv<TEventType>, TEventType>(cookie)
        {
        }
    };

    template <typename TEv, ui32 TEventType>
    class TEventSimple: public TEventBase<TEv, TEventType> {
    public:
        TString ToStringHeader() const override {
            static TString header(TypeName<TEv>());
            return header;
        }

        bool SerializeToArcadiaStream(TChunkSerializer* /*serializer*/) const override {
            static_assert(sizeof(TEv) == sizeof(TEventSimple<TEv, TEventType>), "Descendant should be an empty class");
            return true;
        }

        bool IsSerializable() const override {
            return true;
        }

        static IEventBase* Load(NActors::TEventSerializedData*) {
            return new TEv();
        }

        static IEventBase* Load(const TString&) {
            return new TEv();
        }
    };
}
