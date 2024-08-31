#pragma once

#include "defs.h"
#include "actorid.h"
#include "callstack.h"
#include "event_load.h"

#include <ydb/library/actors/wilson/wilson_trace.h>

#include <util/system/hp_timer.h>
#include <util/generic/maybe.h>

namespace NActors {
    class TChunkSerializer;
    class IActor;
    class ISerializerToStream {
    public:
        virtual bool SerializeToArcadiaStream(TChunkSerializer*) const = 0;
    };

    class IEventBase
        : TNonCopyable,
          public ISerializerToStream {
    protected:
        // for compatibility with virtual actors
        virtual bool DoExecute(IActor* /*actor*/, std::unique_ptr<IEventHandle> /*eventPtr*/) {
            Y_DEBUG_ABORT_UNLESS(false);
            return false;
        }
    public:
        // actual typing is performed by IEventHandle

        virtual ~IEventBase() {
        }

        bool Execute(IActor* actor, std::unique_ptr<IEventHandle> eventPtr) {
            return DoExecute(actor, std::move(eventPtr));
        }

        virtual TString ToStringHeader() const = 0;
        virtual TString ToString() const {
            return ToStringHeader();
        }
        virtual ui32 CalculateSerializedSize() const {
            return 0;
        }
        virtual ui32 Type() const = 0;
        virtual bool SerializeToArcadiaStream(TChunkSerializer*) const = 0;
        virtual bool IsSerializable() const = 0;
        virtual ui32 CalculateSerializedSizeCached() const {
            return CalculateSerializedSize();
        }
        virtual TEventSerializationInfo CreateSerializationInfo() const { return {}; }
    };

    template <typename TEventType>
    class TEventHandle;

    // fat handle
    class IEventHandle : TNonCopyable {
        struct TOnNondelivery {
            TActorId Recipient;

            TOnNondelivery(const TActorId& recipient)
                : Recipient(recipient)
            {
            }
        };

    public:
        template <typename TEv>
        inline TEv* CastAsLocal() const noexcept {
            auto fits = GetTypeRewrite() == TEv::EventType;

            return fits ? static_cast<TEv*>(Event.Get()) : nullptr;
        }

        template <typename TEv>
        inline TEv* StaticCastAsLocal() const noexcept {  // blind cast
            return static_cast<TEv*>(Event.Get());
        }

        template <typename TEv>
        static TAutoPtr<TEventHandle<TEv>> Downcast(TAutoPtr<IEventHandle>&& ev) {
            Y_DEBUG_ABORT_UNLESS(ev->GetTypeRewrite() == TEv::EventType);

            return static_cast<typename TEv::THandle*>(ev.Release());
        };

        template <typename TEv>
        static TAutoPtr<IEventHandle> Upcast(TAutoPtr<TEventHandle<TEv>>&& ev) {
            return ev.Release();
        };

        template <typename TEventType>
        TEventType* Get() {
            if (Type != TEventType::EventType)
                Y_ABORT("Event type %" PRIu32 " doesn't match the expected type %" PRIu32, Type, TEventType::EventType);

            if (!Event) {
                static TEventSerializedData empty;
                Event.Reset(TEventType::Load(Buffer ? Buffer.Get() : &empty));
            }

            if (Event) {
                return static_cast<TEventType*>(Event.Get());
            }

            Y_ABORT("Failed to Load() event type %" PRIu32 " class %s", Type, TypeName<TEventType>().data());
        }

        template <typename T>
        TAutoPtr<T> Release() {
            TAutoPtr<T> x = Get<T>();
            Y_UNUSED(Event.Release());
            Buffer.Reset();
            return x;
        }

        enum EFlags: ui32 {
            FlagTrackDelivery = 1 << 0,
            FlagForwardOnNondelivery = 1 << 1,
            FlagSubscribeOnSession = 1 << 2,
            FlagUseSubChannel = 1 << 3,
            FlagGenerateUnsureUndelivered = 1 << 4,
            FlagExtendedFormat = 1 << 5,
        };
        using TEventFlags = ui32;

        const ui32 Type;
        const TEventFlags Flags;
        const TActorId Recipient;
        TActorId Sender;
        const ui64 Cookie;
        const TScopeId OriginScopeId = TScopeId::LocallyGenerated; // filled in when the message is received from Interconnect

        // if set, used by ActorSystem/Interconnect to report tracepoints
        NWilson::TTraceId TraceId;

        // filled if feeded by interconnect session
        const TActorId InterconnectSession;

#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        ::NHPTimer::STime SendTime;
#endif

        static const size_t ChannelBits = 12;
        static const size_t ChannelShift = (sizeof(ui32) << 3) - ChannelBits;

#ifdef USE_ACTOR_CALLSTACK
        TCallstack Callstack;
#endif
        ui16 GetChannel() const noexcept {
            return Flags >> ChannelShift;
        }

        ui64 GetSubChannel() const noexcept {
            return Flags & FlagUseSubChannel ? Sender.LocalId() : 0ULL;
        }

        static ui32 MakeFlags(ui32 channel, TEventFlags flags) {
            Y_ABORT_UNLESS(channel < (1 << ChannelBits));
            Y_ABORT_UNLESS(flags < (1 << ChannelShift));
            return (flags | (channel << ChannelShift));
        }

    private:
        THolder<IEventBase> Event;
        TIntrusivePtr<TEventSerializedData> Buffer;

        TActorId RewriteRecipient;
        ui32 RewriteType;

        THolder<TOnNondelivery> OnNondeliveryHolder; // only for local events

    public:
        void Rewrite(ui32 typeRewrite, TActorId recipientRewrite) {
            RewriteRecipient = recipientRewrite;
            RewriteType = typeRewrite;
        }

        void DropRewrite() {
            RewriteRecipient = Recipient;
            RewriteType = Type;
        }

        const TActorId& GetRecipientRewrite() const {
            return RewriteRecipient;
        }

        ui32 GetTypeRewrite() const {
            return RewriteType;
        }

        TActorId GetForwardOnNondeliveryRecipient() const {
            return OnNondeliveryHolder.Get() ? OnNondeliveryHolder->Recipient : TActorId();
        }

        IEventHandle(const TActorId& recipient, const TActorId& sender, IEventBase* ev, TEventFlags flags = 0, ui64 cookie = 0,
                     const TActorId* forwardOnNondelivery = nullptr, NWilson::TTraceId traceId = {})
            : Type(ev->Type())
            , Flags(flags)
            , Recipient(recipient)
            , Sender(sender)
            , Cookie(cookie)
            , TraceId(std::move(traceId))
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
            , SendTime(0)
#endif
            , Event(ev)
            , RewriteRecipient(Recipient)
            , RewriteType(Type)
        {
            if (forwardOnNondelivery)
                OnNondeliveryHolder.Reset(new TOnNondelivery(*forwardOnNondelivery));
        }

        IEventHandle(ui32 type,
                     TEventFlags flags,
                     const TActorId& recipient,
                     const TActorId& sender,
                     TIntrusivePtr<TEventSerializedData> buffer,
                     ui64 cookie,
                     const TActorId* forwardOnNondelivery = nullptr,
                     NWilson::TTraceId traceId = {})
            : Type(type)
            , Flags(flags)
            , Recipient(recipient)
            , Sender(sender)
            , Cookie(cookie)
            , TraceId(std::move(traceId))
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
            , SendTime(0)
#endif
            , Buffer(std::move(buffer))
            , RewriteRecipient(Recipient)
            , RewriteType(Type)
        {
            if (forwardOnNondelivery)
                OnNondeliveryHolder.Reset(new TOnNondelivery(*forwardOnNondelivery));
        }

        // Special ctor for events from interconnect.
        IEventHandle(const TActorId& session,
                     ui32 type,
                     TEventFlags flags,
                     const TActorId& recipient,
                     const TActorId& sender,
                     TIntrusivePtr<TEventSerializedData> buffer,
                     ui64 cookie,
                     TScopeId originScopeId,
                     NWilson::TTraceId traceId) noexcept
            : Type(type)
            , Flags(flags)
            , Recipient(recipient)
            , Sender(sender)
            , Cookie(cookie)
            , OriginScopeId(originScopeId)
            , TraceId(std::move(traceId))
            , InterconnectSession(session)
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
            , SendTime(0)
#endif
            , Buffer(std::move(buffer))
            , RewriteRecipient(Recipient)
            , RewriteType(Type)
        {
        }

        TIntrusivePtr<TEventSerializedData> GetChainBuffer();
        TIntrusivePtr<TEventSerializedData> ReleaseChainBuffer();

        ui32 GetSize() const {
            if (Buffer) {
                return Buffer->GetSize();
            } else if (Event) {
                return Event->CalculateSerializedSize();
            } else {
                return 0;
            }
        }

        bool HasBuffer() const {
            return bool(Buffer);
        }

        bool HasEvent() const {
            return bool(Event);
        }

        IEventBase* GetBase() {
            if (!Event) {
                if (!Buffer)
                    return nullptr;
                else
                    ythrow TWithBackTrace<yexception>() << "don't know how to load the event from buffer";
            }

            return Event.Get();
        }

        TAutoPtr<IEventBase> ReleaseBase() {
            TAutoPtr<IEventBase> x = GetBase();
            Y_UNUSED(Event.Release());
            Buffer.Reset();
            return x;
        }

        TAutoPtr<IEventHandle> Forward(const TActorId& dest) {
            if (Event)
                return new IEventHandle(dest, Sender, Event.Release(), Flags, Cookie, nullptr, std::move(TraceId));
            else
                return new IEventHandle(Type, Flags, dest, Sender, Buffer, Cookie, nullptr, std::move(TraceId));
        }

        TString GetTypeName() const;
        TString ToString() const;

        [[nodiscard]] static std::unique_ptr<IEventHandle> Forward(std::unique_ptr<IEventHandle>&& ev, TActorId recipient);
        [[nodiscard]] static std::unique_ptr<IEventHandle> ForwardOnNondelivery(std::unique_ptr<IEventHandle>&& ev, ui32 reason, bool unsure = false);

        [[nodiscard]] static TAutoPtr<IEventHandle> Forward(TAutoPtr<IEventHandle>&& ev, TActorId recipient) {
            return Forward(std::unique_ptr<IEventHandle>(ev.Release()), recipient).release();
        }

        [[nodiscard]] static THolder<IEventHandle> Forward(THolder<IEventHandle>&& ev, TActorId recipient) {
            return THolder(Forward(std::unique_ptr<IEventHandle>(ev.Release()), recipient).release());
        }

        [[nodiscard]] static TAutoPtr<IEventHandle> ForwardOnNondelivery(TAutoPtr<IEventHandle>&& ev, ui32 reason, bool unsure = false) {
            return ForwardOnNondelivery(std::unique_ptr<IEventHandle>(ev.Release()), reason, unsure).release();
        }

        [[nodiscard]] static THolder<IEventHandle> ForwardOnNondelivery(THolder<IEventHandle>&& ev, ui32 reason, bool unsure = false) {
            return THolder(ForwardOnNondelivery(std::unique_ptr<IEventHandle>(ev.Release()), reason, unsure).release());
        }

        template<typename T>
        static TAutoPtr<T> Release(TAutoPtr<IEventHandle>& ev) {
            return ev->Release<T>();
        }

        template<typename T>
        static TAutoPtr<T> Release(THolder<IEventHandle>& ev) {
            return ev->Release<T>();
        }
    };

    template <typename TEventType>
    class TEventHandle: public IEventHandle {
        TEventHandle(); // we never made instance of TEventHandle
    public:
        TEventType* Get() {
            return IEventHandle::Get<TEventType>();
        }

        TAutoPtr<TEventType> Release() {
            return IEventHandle::Release<TEventType>();
        }
    };

    static_assert(sizeof(TEventHandle<IEventBase>) == sizeof(IEventHandle), "expect sizeof(TEventHandle<IEventBase>) == sizeof(IEventHandle)");

    template <typename TEventType, ui32 EventType0>
    class TEventBase: public IEventBase {
    public:
        static constexpr ui32 EventType = EventType0;
        ui32 Type() const override {
            return EventType0;
        }
        // still abstract

        typedef TEventHandle<TEventType> THandle;
        typedef TAutoPtr<THandle> TPtr;
    };

#define DEFINE_SIMPLE_NONLOCAL_EVENT(eventType, header)                 \
    TString ToStringHeader() const override {                           \
        return TString(header);                                         \
    }                                                                   \
    bool SerializeToArcadiaStream(NActors::TChunkSerializer*) const override { \
        return true;                                                    \
    }                                                                   \
    static IEventBase* Load(NActors::TEventSerializedData*) {           \
        return new eventType();                                         \
    }                                                                   \
    bool IsSerializable() const override {                              \
        return true;                                                    \
    }
}
