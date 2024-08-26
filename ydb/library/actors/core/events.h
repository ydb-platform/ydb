#pragma once

#include "event_local.h"
#include "event_pb.h"

#include <util/system/unaligned_mem.h>

namespace NActorsProto {
    class TCallbackException;
    class TActorId;
} // NActorsProto

namespace NActors {
    struct TEvents {
        enum EEventSpace {
            ES_HELLOWORLD = 0,
            ES_SYSTEM = 1,
            ES_INTERCONNECT = 2,
            ES_INTERCONNECT_MSGBUS = 3,
            ES_DNS = 4,
            ES_SOCKET_POLLER = 5,
            ES_LOGGER = 6,
            ES_MON = 7,
            ES_INTERCONNECT_TCP = 8,
            ES_PROFILER = 9,
            ES_YF = 10,
            ES_HTTP = 11,
            ES_PGWIRE = 12,

            ES_USERSPACE = 4096,

            ES_PRIVATE = (1 << 15) - 16,
            ES_MAX = (1 << 15),
        };

#define EventSpaceBegin(eventSpace) (eventSpace << 16u)
#define EventSpaceEnd(eventSpace) ((eventSpace << 16u) + (1u << 16u))

        struct THelloWorld {
            enum {
                Start = EventSpaceBegin(ES_HELLOWORLD),
                Ping,
                Pong,
                Blob,
                End
            };

            static_assert(End < EventSpaceEnd(ES_HELLOWORLD), "expect End < EventSpaceEnd(ES_HELLOWORLD)");
        };

        struct TEvPing: public TEventBase<TEvPing, THelloWorld::Ping> {
            DEFINE_SIMPLE_NONLOCAL_EVENT(TEvPing, "HelloWorld: Ping");
        };

        struct TEvPong: public TEventBase<TEvPong, THelloWorld::Pong> {
            DEFINE_SIMPLE_NONLOCAL_EVENT(TEvPong, "HelloWorld: Pong");
        };

        struct TEvBlob: public TEventBase<TEvBlob, THelloWorld::Blob> {
            const TString Blob;

            TEvBlob(const TString& blob) noexcept
                : Blob(blob)
            {
            }

            TString ToStringHeader() const noexcept override {
                return "THelloWorld::Blob";
            }

            bool SerializeToArcadiaStream(TChunkSerializer *serializer) const override {
                return serializer->WriteString(&Blob);
            }

            static IEventBase* Load(TEventSerializedData* bufs) noexcept {
                return new TEvBlob(bufs->GetString());
            }

            bool IsSerializable() const override {
                return true;
            }
        };

        struct TSystem {
            enum {
                Start = EventSpaceBegin(ES_SYSTEM),
                Bootstrap,   // generic bootstrap event
                Wakeup,      // generic timeout
                Subscribe,   // generic subscribe to something
                Unsubscribe, // generic unsubscribe from something
                Delivered,   // event delivered
                Undelivered, // event undelivered
                Poison,      // request actor to shutdown
                Completed,   // generic async job result event
                PoisonTaken, // generic Poison taken (reply to PoisonPill event, i.e. died completely)
                FlushLog,
                CallbackCompletion,
                CallbackException,
                Gone,        // Generic notification of actor death
                TrackActor,
                UntrackActor,
                InvokeResult,
                CoroTimeout,
                InvokeQuery,
                Wilson,
                End,

                // Compatibility section
                PoisonPill = Poison,
                ActorDied = Gone,
            };

            static_assert(End < EventSpaceEnd(ES_SYSTEM), "expect End < EventSpaceEnd(ES_SYSTEM)");
        };

        struct TEvBootstrap: public TEventLocal<TEvBootstrap, TSystem::Bootstrap> {
        };

        struct TEvPoison : public TEventBase<TEvPoison, TSystem::Poison> {
            DEFINE_SIMPLE_NONLOCAL_EVENT(TEvPoison, "System: TEvPoison")
        };

        struct TEvWakeup: public TEventLocal<TEvWakeup, TSystem::Wakeup> {
            TEvWakeup(ui64 tag = 0) : Tag(tag) { }

            const ui64 Tag = 0;
        };

        struct TEvSubscribe: public TEventLocal<TEvSubscribe, TSystem::Subscribe> {
        };

        struct TEvUnsubscribe: public TEventLocal<TEvUnsubscribe, TSystem::Unsubscribe> {
        };

        struct TEvUndelivered: public TEventBase<TEvUndelivered, TSystem::Undelivered> {
            enum EReason {
                ReasonUnknown,
                ReasonActorUnknown,
                Disconnected
            };
            const ui32 SourceType;
            const EReason Reason;
            const bool Unsure;
            const TString Data;

            TEvUndelivered(ui32 sourceType, ui32 reason, bool unsure = false)
                : SourceType(sourceType)
                , Reason(static_cast<EReason>(reason))
                , Unsure(unsure)
                , Data(MakeData(sourceType, reason))
            {}

            TString ToStringHeader() const override;
            bool SerializeToArcadiaStream(TChunkSerializer *serializer) const override;
            static IEventBase* Load(TEventSerializedData* bufs);
            bool IsSerializable() const override;

            ui32 CalculateSerializedSize() const override { return 2 * sizeof(ui32); }

            static void Out(IOutputStream& o, EReason x);

        private:
            static TString MakeData(ui32 sourceType, ui32 reason) {
                TString s = TString::Uninitialized(sizeof(ui32) + sizeof(ui32));
                char *p = s.Detach();
                WriteUnaligned<ui32>(p + 0, sourceType);
                WriteUnaligned<ui32>(p + 4, reason);
                return s;
            }
        };

        struct TEvCompleted: public TEventLocal<TEvCompleted, TSystem::Completed> {
            const ui32 Id;
            const ui32 Status;
            TEvCompleted(ui32 id = 0, ui32 status = 0)
                : Id(id)
                , Status(status)
            {
            }
        };

        struct TEvPoisonTaken: public TEventLocal<TEvPoisonTaken, TSystem::PoisonTaken> {
        };

        struct TEvFlushLog: public TEventLocal<TEvFlushLog, TSystem::FlushLog> {
        };

        struct TEvCallbackException : TEventLocal<TEvCallbackException, TSystem::CallbackException> {
            const TActorId Id;
            TString Msg;

            TEvCallbackException(const TActorId& id, const TString& msg) : Id(id), Msg(msg) {}
        };

        struct TEvCallbackCompletion : TEventLocal<TEvCallbackCompletion, TSystem::CallbackCompletion> {
            const TActorId Id;

            TEvCallbackCompletion(const TActorId& id) : Id(id) {}
        };

        struct TEvGone: public TEventLocal<TEvGone, TSystem::Gone> {
        };

        struct TEvInvokeResult;

        using TEvPoisonPill = TEvPoison; // Legacy name, deprecated
        using TEvActorDied = TEvGone;
    };
}

template <>
inline void Out<NActors::TEvents::TEvUndelivered::EReason>(IOutputStream& o, NActors::TEvents::TEvUndelivered::EReason x) {
    NActors::TEvents::TEvUndelivered::Out(o, x);
}
