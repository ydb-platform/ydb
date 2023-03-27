#pragma once

#include "defs.h"
#include "actorid.h"
#include "callstack.h"
#include "event_load.h"

#include <library/cpp/actors/wilson/wilson_trace.h>

#include <util/system/hp_timer.h>
#include <util/generic/maybe.h>
#include <util/string/builder.h>

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
        virtual bool DoExecute(IActor* /*actor*/, std::unique_ptr<IEventHandleFat> /*eventPtr*/) {
            Y_VERIFY_DEBUG(false);
            return false;
        }
    public:
        // actual typing is performed by IEventHandle

        virtual ~IEventBase() {
        }

        bool Execute(IActor* actor, std::unique_ptr<IEventHandleFat> eventPtr) {
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

    struct IEventHandleFields {
        enum EFlags : ui32 {
            FlagTrackDelivery = 1 << 0,
            FlagForwardOnNondelivery = 1 << 1,
            FlagSubscribeOnSession = 1 << 2,
            FlagUseSubChannel = 1 << 3,
            FlagGenerateUnsureUndelivered = 1 << 4,
            FlagExtendedFormat = 1 << 5,
            FlagLight = 1 << 18,
            FlagSerializable = 1 << 19,
        };

        static constexpr ui32 STICKY_FLAGS = (FlagLight | FlagSerializable);

        TActorId Recipient = {};
        TActorId Sender = {};
        ui32 Type = {};

#pragma pack(push, 1)
        union {
            ui32 Flags = {};
            struct {
                ui32 NormalFlags:6;
                ui32 ReservedFlags:12;
                ui32 StickyFlags:2;
                ui32 Channel:12;
            };
            struct {
                // flags
                ui32 TrackDelivery:1;
                ui32 ForwardOnNondeliveryFlag:1;
                ui32 SubscribeOnSession:1;
                ui32 UseSubChannel:1;
                ui32 GenerateUnsureUndelivered:1;
                ui32 ExtendedFormat:1;
                // reserved
                ui32 Reserved:12;
                // sticky flags
                ui32 Light:1;
                ui32 Serializable:1;
            };
        };
#pragma pack(pop)

        ui64 Cookie = {};

        TActorId RewriteRecipient = {};
        ui32 RewriteType = {};

        NWilson::TTraceId TraceId = {};

#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        ::NHPTimer::STime SendTime = 0;
#endif
    };

    class IEventHandle : public IEventHandleFields {
    public:
        IEventHandle(IEventHandleFields&& fields = {})
            : IEventHandleFields(std::move(fields))
        {}

        virtual ~IEventHandle() = default;

        struct TOnNondelivery {
            TActorId Recipient;

            TOnNondelivery(const TActorId& recipient)
                : Recipient(recipient)
            {
            }
        };

        THolder<TOnNondelivery> OnNondeliveryHolder; // only for local events

        ui16 GetChannel() const noexcept {
            return Channel;
        }

        ui64 GetSubChannel() const noexcept {
            return UseSubChannel ? Sender.LocalId() : 0ULL;
        }

        // deprecate(xenoxeno) ?
        static const size_t ChannelBits = 12;
        static const size_t ChannelShift = (sizeof(ui32) << 3) - ChannelBits;

        static ui32 MakeFlags(ui32 channel, ui32 flags) {
            Y_VERIFY(channel < (1 << ChannelBits));
            Y_VERIFY(flags < (1 << ChannelShift));
            return (flags | (channel << ChannelShift));
        }
        //

        void SetFlags(ui32 flags) {
            Flags = (flags & ~STICKY_FLAGS) | (Flags & STICKY_FLAGS);
        }

        const TActorId& GetRecipientRewrite() const {
            return RewriteRecipient;
        }

        void Rewrite(ui32 typeRewrite, TActorId recipientRewrite) {
            RewriteRecipient = recipientRewrite;
            RewriteType = typeRewrite;
        }

        void DropRewrite() {
            RewriteRecipient = Recipient;
            RewriteType = Type;
        }

        ui32 GetTypeRewrite() const {
            return RewriteType;
        }

        bool IsEventLight() const {
            return Light;
        }

        bool IsEventFat() const {
            return !Light;
        }

        bool IsEventSerializable() const {
            return Serializable;
        }

        template<typename TEventType>
        TEventType* Get();
        template<typename TEventType>
        TEventType* CastAsLocal();
        template<typename TEventType>
        TEventType* StaticCastAsLocal();
        bool HasEvent() const;
        bool HasBuffer() const;
        TString GetTypeName() const;
        TString ToString() const;
        size_t GetSize() const;
        static TAutoPtr<IEventHandle>& Forward(TAutoPtr<IEventHandle>& ev, TActorId recipient);
        static THolder<IEventHandle>& Forward(THolder<IEventHandle>& ev, TActorId recipient);
        static TAutoPtr<IEventHandle> ForwardOnNondelivery(TAutoPtr<IEventHandle>& ev, ui32 reason, bool unsure = false);
        static TAutoPtr<IEventHandle> ForwardOnNondelivery(std::unique_ptr<IEventHandle>& ev, ui32 reason, bool unsure = false);
        template<typename TEventType>
        static TEventType* Release(TAutoPtr<IEventHandle>&);
        template<typename TEventType>
        static TEventType* Release(THolder<IEventHandle>&);
        template<typename TEventType>
        static TEventType* Release(std::unique_ptr<IEventHandle>&);

        template<typename TEventTypeSmartPtr>
        static TAutoPtr<IEventHandle> ForwardOnNondelivery(TEventTypeSmartPtr& ev, ui32 reason, bool unsure = false) {
            TAutoPtr<IEventHandle> evi(ev.Release());
            return ForwardOnNondelivery(evi, reason, unsure);
        }

        TActorId GetForwardOnNondeliveryRecipient() const;
    };

    // fat handle
    class IEventHandleFat : public IEventHandle, TNonCopyable {
    public:
        template <typename TEv>
        inline TEv* CastAsLocal() const noexcept { // cast with event check
            auto fits = GetTypeRewrite() == TEv::EventType;
            constexpr bool couldBeCasted = requires() {static_cast<TEv*>(Event.Get());};
            if constexpr (couldBeCasted) {
                return fits ? static_cast<TEv*>(Event.Get()) : nullptr;
            } else {
                Y_FAIL("Event type %" PRIu32 " couldn't be converted to type %s", Type, TypeName<TEv>().data());
            }
        }

        template <typename TEv>
        inline TEv* StaticCastAsLocal() const noexcept { // blind cast
            constexpr bool couldBeCasted = requires() {static_cast<TEv*>(Event.Get());};
            if constexpr (couldBeCasted) {
                return static_cast<TEv*>(Event.Get());
            } else {
                Y_FAIL("Event type %" PRIu32 " couldn't be converted to type %s", Type, TypeName<TEv>().data());
            }
        }

        template <typename TEventType>
        TEventType* Get() {
            if (Type != TEventType::EventType)
                Y_FAIL("%s", (TStringBuilder()
                    << "Event type " << Type
                    << " doesn't match the expected type " << TEventType::EventType
                    << " requested typename " << TypeName<TEventType>()
                    << " actual typename " << GetTypeName()).data());

            constexpr bool couldBeGot = requires() {
                Event.Reset(TEventType::Load(Buffer.Get()));
                static_cast<TEventType*>(Event.Get());
            };
            if constexpr (couldBeGot) {
                if (!Event) {
                    static TEventSerializedData empty;
                    Event.Reset(TEventType::Load(Buffer ? Buffer.Get() : &empty));
                }

                if (Event) {
                    return static_cast<TEventType*>(Event.Get());
                }

                Y_FAIL("Failed to Load() event type %" PRIu32 " class %s", Type, TypeName<TEventType>().data());
            } else {
                Y_FAIL("Event type %" PRIu32 " couldn't be get as type %s", Type, TypeName<TEventType>().data());
            }
        }

        template <typename T>
        TAutoPtr<T> Release() {
            TAutoPtr<T> x = Get<T>();
            Y_UNUSED(Event.Release());
            Buffer.Reset();
            return x;
        }

        const TScopeId OriginScopeId = TScopeId::LocallyGenerated; // filled in when the message is received from Interconnect

        // filled if feeded by interconnect session
        const TActorId InterconnectSession;


#ifdef USE_ACTOR_CALLSTACK
        TCallstack Callstack;
#endif
    private:
        THolder<IEventBase> Event;
        TIntrusivePtr<TEventSerializedData> Buffer;

    public:
        TActorId GetForwardOnNondeliveryRecipient() const {
            return OnNondeliveryHolder.Get() ? OnNondeliveryHolder->Recipient : TActorId();
        }

        IEventHandleFat(const TActorId& recipient, const TActorId& sender, IEventBase* ev, ui32 flags = 0, ui64 cookie = 0,
                     const TActorId* forwardOnNondelivery = nullptr, NWilson::TTraceId traceId = {})
            : IEventHandle({
                .Recipient = recipient,
                .Sender = sender,
                .Type = ev->Type(),
                .Flags = flags,
                .Cookie = cookie,
                .RewriteRecipient = recipient,
                .RewriteType = ev->Type(),
                .TraceId = std::move(traceId),
            })
            , Event(ev)
        {
            if (forwardOnNondelivery)
                OnNondeliveryHolder.Reset(new TOnNondelivery(*forwardOnNondelivery));
        }

        IEventHandleFat(ui32 type,
                     ui32 flags,
                     const TActorId& recipient,
                     const TActorId& sender,
                     TIntrusivePtr<TEventSerializedData> buffer,
                     ui64 cookie,
                     const TActorId* forwardOnNondelivery = nullptr,
                     NWilson::TTraceId traceId = {})
            : IEventHandle({
                .Recipient = recipient,
                .Sender = sender,
                .Type = type,
                .Flags = flags,
                .Cookie = cookie,
                .RewriteRecipient = recipient,
                .RewriteType = type,
                .TraceId = std::move(traceId),
            })
            , Buffer(std::move(buffer))
        {
            if (forwardOnNondelivery)
                OnNondeliveryHolder.Reset(new TOnNondelivery(*forwardOnNondelivery));
        }

        // Special ctor for events from interconnect.
        IEventHandleFat(const TActorId& session,
                     ui32 type,
                     ui32 flags,
                     const TActorId& recipient,
                     const TActorId& sender,
                     TIntrusivePtr<TEventSerializedData> buffer,
                     ui64 cookie,
                     TScopeId originScopeId,
                     NWilson::TTraceId traceId) noexcept
            : IEventHandle({
                .Recipient = recipient,
                .Sender = sender,
                .Type = type,
                .Flags = flags,
                .Cookie = cookie,
                .RewriteRecipient = recipient,
                .RewriteType = type,
                .TraceId = std::move(traceId),
            })
            , OriginScopeId(originScopeId)
            , InterconnectSession(session)
            , Buffer(std::move(buffer))
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
                return new IEventHandleFat(dest, Sender, Event.Release(), Flags, Cookie, nullptr, std::move(TraceId));
            else
                return new IEventHandleFat(Type, Flags, dest, Sender, Buffer, Cookie, nullptr, std::move(TraceId));
        }

        static TAutoPtr<IEventHandleFat> MakeFat(TAutoPtr<IEventHandle> ev) {
            if (ev->IsEventLight()) {
                Y_FAIL("Can't make light event fat");
            } else {
                TAutoPtr<IEventHandleFat> evb(static_cast<IEventHandleFat*>(ev.Release()));
                return evb;
            }
        }

        static IEventHandleFat* GetFat(IEventHandle* ev) {
            if (ev->IsEventLight()) {
                Y_FAIL("Can't make light event fat");
            } else {
                return static_cast<IEventHandleFat*>(ev);
            }
        }

        static const IEventHandleFat* GetFat(const IEventHandle* ev) {
            if (ev->IsEventLight()) {
                Y_FAIL("Can't make light event fat");
            } else {
                return static_cast<const IEventHandleFat*>(ev);
            }
        }

        static IEventHandleFat* GetFat(TAutoPtr<IEventHandle>& ev) {
            return GetFat(ev.Get());
        }

        static IEventHandleFat* GetFat(THolder<IEventHandle>& ev) {
            return GetFat(ev.Get());
        }

        static IEventHandleFat* GetFat(std::unique_ptr<IEventHandle>& ev) {
            return GetFat(ev.get());
        }

        static TAutoPtr<IEventHandle> MakeBase(TAutoPtr<IEventHandleFat> ev) {
            return ev.Release();
        }

        static std::unique_ptr<IEventHandle> ForwardOnNondelivery(std::unique_ptr<IEventHandleFat>& ev, ui32 reason, bool unsure = false);
    };

    template <typename TEventType>
    class TEventHandleFat: public IEventHandleFat {
        TEventHandleFat(); // we never made instance of TEventHandleFat
    public:
        TEventType* Get() {
            return IEventHandleFat::Get<TEventType>();
        }

        TAutoPtr<TEventType> Release() {
            return IEventHandleFat::Release<TEventType>();
        }
    };

    static_assert(sizeof(TEventHandleFat<IEventBase>) == sizeof(IEventHandleFat), "expect sizeof(TEventHandleFat<IEventBase>) == sizeof(IEventHandleFat)");

    // light handle
    class IEventHandleLight : public IEventHandle {
    public:
        IEventHandleLight(ui32 type) {
            RewriteType = Type = type;
            Light = true;
        }

        IEventHandleLight* PrepareSend(TActorId recipient, TActorId sender) {
            RewriteRecipient = Recipient = recipient;
            Sender = sender;
            return this;
        }

        IEventHandleLight* PrepareSend(TActorId recipient, TActorId sender, ui32 flags) {
            RewriteRecipient = Recipient = recipient;
            Sender = sender;
            SetFlags(flags);
            return this;
        }

        IEventHandleLight* PrepareSend(TActorId recipient, TActorId sender, ui32 flags, ui64 cookie) {
            RewriteRecipient = Recipient = recipient;
            Sender = sender;
            SetFlags(flags);
            Cookie = cookie;
            return this;
        }

        IEventHandleLight* PrepareSend(TActorId recipient, TActorId sender, ui32 flags, ui64 cookie, NWilson::TTraceId traceId) {
            RewriteRecipient = Recipient = recipient;
            Sender = sender;
            SetFlags(flags);
            Cookie = cookie;
            TraceId = std::move(traceId);
            return this;
        }

        void Forward(const TActorId& dest) {
            RewriteRecipient = dest;
        }

        static IEventHandleLight* GetLight(IEventHandle* ev) {
            if (ev->IsEventLight()) {
                return static_cast<IEventHandleLight*>(ev);
            } else {
                Y_FAIL("Can't make fat event light");
            }
        }

        static IEventHandleLight* GetLight(TAutoPtr<IEventHandle>& ev) {
            return GetLight(ev.Get());
        }

        static IEventHandleLight* GetLight(THolder<IEventHandle>& ev) {
            return GetLight(ev.Get());
        }

        static IEventHandleLight* GetLight(std::unique_ptr<IEventHandle>& ev) {
            return GetLight(ev.get());
        }

        static IEventHandleLight* ReleaseLight(TAutoPtr<IEventHandle>& ev) {
            return GetLight(ev.Release());
        }

        static std::unique_ptr<IEventHandle> ForwardOnNondelivery(std::unique_ptr<IEventHandleLight>& ev, ui32 reason, bool unsure = false);

        template<typename TEventType>
        TEventType* Get() {
            if (Type != TEventType::EventType) {
                Y_FAIL("Event type %" PRIu32 " doesn't match the expected type %" PRIu32, Type, TEventType::EventType);
            }
            constexpr bool couldBeConverted = requires() {static_cast<TEventType*>(this);};
            if constexpr (couldBeConverted) {
                return static_cast<TEventType*>(this);
            } else {
                Y_FAIL("Event type %" PRIu32 " couldn't be converted to type %s", Type, TypeName<TEventType>().data());
            }
        }

        template<typename TEventType>
        TEventType* CastAsLocal() {
            constexpr bool couldBeConverted = requires() {static_cast<TEventType*>(this);};
            if constexpr (couldBeConverted) {
                if (Type == TEventType::EventType) {
                    return static_cast<TEventType*>(this);
                }
            }
            return nullptr;
        }

        template<typename TEventType>
        TEventType* StaticCastAsLocal() {
            constexpr bool couldBeConverted = requires() {static_cast<TEventType*>(this);};
            if constexpr (couldBeConverted) {
                return static_cast<TEventType*>(this);
            }
            Y_FAIL("Event type %" PRIu32 " couldn't be converted to type %s", Type, TypeName<TEventType>().data());
        }
    };

    template<typename TEventType>
    TEventType* IEventHandle::Get() {
        if (IsEventLight()) {
            return IEventHandleLight::GetLight(this)->Get<TEventType>();
        } else {
            return IEventHandleFat::GetFat(this)->Get<TEventType>();
        }
    }

    template<typename TEventType>
    TEventType* IEventHandle::CastAsLocal() {
        if (IsEventLight()) {
            return IEventHandleLight::GetLight(this)->CastAsLocal<TEventType>();
        } else {
            return IEventHandleFat::GetFat(this)->CastAsLocal<TEventType>();
        }
    }

    template<typename TEventType>
    TEventType* IEventHandle::StaticCastAsLocal() {
        if (IsEventLight()) {
            return IEventHandleLight::GetLight(this)->StaticCastAsLocal<TEventType>();
        } else {
            return IEventHandleFat::GetFat(this)->StaticCastAsLocal<TEventType>();
        }
    }

    template<typename TEventType>
    TEventType* IEventHandle::Release(TAutoPtr<IEventHandle>& ev) {
        if (ev->IsEventLight()) {
            return IEventHandleLight::GetLight(ev.Release())->CastAsLocal<TEventType>();
        } else {
            return IEventHandleFat::GetFat(ev.Get())->Release<TEventType>().Release();
        }
    }

    template<typename TEventType>
    TEventType* IEventHandle::Release(THolder<IEventHandle>& ev) {
        if (ev->IsEventLight()) {
            return IEventHandleLight::GetLight(ev.Release())->CastAsLocal<TEventType>();
        } else {
            return IEventHandleFat::GetFat(ev.Get())->Release<TEventType>().Release();
        }
    }

    template<typename TEventType>
    TEventType* IEventHandle::Release(std::unique_ptr<IEventHandle>& ev) {
        if (ev->IsEventLight()) {
            return IEventHandleLight::GetLight(ev.release())->CastAsLocal<TEventType>();
        } else {
            return IEventHandleFat::GetFat(ev.get())->Release<TEventType>().Release();
        }
    }


    class IEventHandleLightSerializable;

    using TEventSerializer = std::function<bool(const IEventHandleLightSerializable*, TChunkSerializer*)>;

    class IEventHandleLightSerializable : public IEventHandleLight {
    public:
        TEventSerializer Serializer;

        IEventHandleLightSerializable(ui32 type, TEventSerializer serializer)
            : IEventHandleLight(type)
            , Serializer(std::move(serializer))
        {
            Serializable = true;
        }

        static IEventHandleLightSerializable* GetLightSerializable(IEventHandle* ev) {
            if (ev->IsEventSerializable()) {
                return static_cast<IEventHandleLightSerializable*>(ev);
            } else {
                Y_FAIL("Can't make serializable event");
            }
        }

        static const IEventHandleLightSerializable* GetLightSerializable(const IEventHandle* ev) {
            if (ev->IsEventSerializable()) {
                return static_cast<const IEventHandleLightSerializable*>(ev);
            } else {
                Y_FAIL("Can't make serializable event");
            }
        }

        static IEventHandleLightSerializable* GetLightSerializable(TAutoPtr<IEventHandle>& ev) {
            return GetLightSerializable(ev.Get());
        }

        size_t GetSize() const {
            // TODO(xenoxeno)
            return 0;
        }
    };

    template <typename TEventType, ui32 EventType0>
    class TEventBase: public IEventBase {
    public:
        static constexpr ui32 EventType = EventType0;
        ui32 Type() const override {
            return EventType0;
        }
        // still abstract

        typedef TEventHandleFat<TEventType> THandle;
        typedef TAutoPtr<THandle> TPtr;
    };

#define DEFINE_SIMPLE_LOCAL_EVENT(eventType, header)                    \
    TString ToStringHeader() const override {                           \
        return TString(header);                                         \
    }                                                                   \
    bool SerializeToArcadiaStream(NActors::TChunkSerializer*) const override { \
        Y_FAIL("Local event %s is not serializable", TypeName(*this).data()); \
    }                                                                   \
    static IEventBase* Load(NActors::TEventSerializedData*) {           \
        Y_FAIL("Local event " #eventType " has no load method");        \
    }                                                                   \
    bool IsSerializable() const override {                              \
        return false;                                                   \
    }

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


    struct TEventMeta {
        TActorId Session;
        ui32 Type;
        ui32 Flags;
        TActorId Recipient;
        TActorId Sender;
        ui64 Cookie;
        TScopeId OriginScopeId;
        NWilson::TTraceId TraceId;
        TRope Data;

        bool IsExtendedFormat() const {
            return Flags & IEventHandle::FlagExtendedFormat;
        }
    };

    inline constexpr ui32 GetEventSpace(ui32 eventType) {
        return (eventType >> 16u);
    }

    inline constexpr ui32 GetEventSubType(ui32 eventType) {
        return (eventType & (0xffff));
    }

    struct IEventFactory {
        virtual IEventHandle* Construct(const TEventMeta& eventMeta) = 0;
        virtual void Destruct(IEventHandle*) = 0;
    };

    struct TEventFactories {
        // it supposed to be statically allocated in the begining
        static std::vector<std::vector<IEventFactory*>*> EventFactories; // [EventSpace][EventSubType]
    };
}
