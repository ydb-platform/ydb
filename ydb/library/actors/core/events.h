#pragma once

#include "event.h"
#include "event_pb.h"

#include <ydb/library/actors/protos/actors.pb.h>
#include <util/system/unaligned_mem.h>

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

        struct TEvBootstrap: public TEventBase<TEvBootstrap, TSystem::Bootstrap> {
            DEFINE_SIMPLE_LOCAL_EVENT(TEvBootstrap, "System: TEvBootstrap")
        };

        struct TEvPoison : public TEventBase<TEvPoison, TSystem::Poison> {
            DEFINE_SIMPLE_NONLOCAL_EVENT(TEvPoison, "System: TEvPoison")
        };

        struct TEvWakeup: public TEventBase<TEvWakeup, TSystem::Wakeup> {
            DEFINE_SIMPLE_LOCAL_EVENT(TEvWakeup, "System: TEvWakeup")

            TEvWakeup(ui64 tag = 0) : Tag(tag) { }

            const ui64 Tag = 0;
        };

        struct TEvSubscribe: public TEventBase<TEvSubscribe, TSystem::Subscribe> {
            DEFINE_SIMPLE_LOCAL_EVENT(TEvSubscribe, "System: TEvSubscribe")
        };

        struct TEvUnsubscribe: public TEventBase<TEvUnsubscribe, TSystem::Unsubscribe> {
            DEFINE_SIMPLE_LOCAL_EVENT(TEvUnsubscribe, "System: TEvUnsubscribe")
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

        struct TEvCompleted: public TEventBase<TEvCompleted, TSystem::Completed> {
            const ui32 Id;
            const ui32 Status;
            TEvCompleted(ui32 id = 0, ui32 status = 0)
                : Id(id)
                , Status(status)
            {
            }

            DEFINE_SIMPLE_LOCAL_EVENT(TEvCompleted, "System: TEvCompleted")
        };

        struct TEvPoisonTaken: public TEventBase<TEvPoisonTaken, TSystem::PoisonTaken> {
            DEFINE_SIMPLE_LOCAL_EVENT(TEvPoisonTaken, "System: TEvPoisonTaken")
        };

        struct TEvFlushLog: public TEventBase<TEvFlushLog, TSystem::FlushLog> {
            DEFINE_SIMPLE_LOCAL_EVENT(TEvFlushLog, "System: TEvFlushLog")
        };

        struct TEvCallbackException: public TEventPB<TEvCallbackException,
                                                      NActorsProto::TCallbackException,
                                                      TSystem::CallbackException> {
            TEvCallbackException(const TActorId& id, const TString& msg) {
                ActorIdToProto(id, Record.MutableActorId());
                Record.SetExceptionMessage(msg);
            }
        };

        struct TEvCallbackCompletion: public TEventPB<TEvCallbackCompletion,
                                                       NActorsProto::TActorId,
                                                       TSystem::CallbackCompletion> {
            TEvCallbackCompletion(const TActorId& id) {
                ActorIdToProto(id, &Record);
            }
        };

        struct TEvGone: public TEventBase<TEvGone, TSystem::Gone> {
            DEFINE_SIMPLE_LOCAL_EVENT(TEvGone, "System: TEvGone")
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
