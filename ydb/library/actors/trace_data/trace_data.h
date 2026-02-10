#pragma once

#include <util/generic/fwd.h>

#include <util/generic/buffer.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>

namespace NActors {
    namespace NTracing {
        struct TLocalActorId {
            ui64 LocalId = 0;
        };
        struct TGlobalActorId {
            ui64 LocalId = 0;
            ui32 NodeId = 0;
        };

        template <typename TTraits>
        struct TSendEvent: public TTraits {
            TTraits::TActorId Sender;
            TTraits::TActorId Recipient;
            ui64 ObjectId = 0;
        };

        template <typename TTraits>
        struct TRecieveEvent : public TTraits {
            TTraits::TActorId Sender;
            TTraits::TActorId Recipient;
            ui64 ObjectId = 0;
            ui32 MessageType = 0;
        };

        struct TLocalTraits {
            using TActorId = TLocalActorId;
        };

        struct TRecieveLocalTraits: public TLocalTraits {
            ui32 ActivityIndex = 0;
        };

        struct TInterconnectTraits {
            using TActorId = TGlobalActorId;
            ui32 InterconnectSequenceId = 0;
        };

        struct TNewEvent {
            TLocalActorId ActorId;
        };

        struct TDieEvent {
            TLocalActorId ActorId;
        };

        struct TEvent {
            enum class EType : ui8 {
                SendInterconnect,
                RecieveInterconnect,
                SendLocal,
                RecieveLocal,
                New,
                Die
            };
            uint64_t Timestamp = 0;
            union TEventImpl {
                TSendEvent<TInterconnectTraits> SendInterconnectEvent;
                TRecieveEvent<TInterconnectTraits> RecieveInterconnectEvent;
                TSendEvent<TLocalTraits> SendLocalEvent;
                TRecieveEvent<TRecieveLocalTraits> RecieveLocalEvent;
                TNewEvent NewEvent;
                TDieEvent DieEvent;
            } Event;
            EType Type;
        };

        using TActivityDict = TVector<TStringBuf>;
        using TEventNamesDict = THashMap<ui32, TString>;

        struct TTraceChunk {
            using TEvents = TVector<TEvent>;
            TActivityDict ActivityDict;
            TEventNamesDict EventNamesDict;
            TEvents Events;
        };

        TBuffer SerializeHeader(TActivityDict&& activityDict, TEventNamesDict&& eventNamesDict);
        TBuffer SerializeEvents(TTraceChunk::TEvents&& events);
    }
}
