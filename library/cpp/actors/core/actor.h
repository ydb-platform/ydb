#pragma once

#include "event.h"
#include "monotonic.h"
#include <util/system/tls.h>
#include <library/cpp/actors/util/local_process_key.h> 

namespace NActors {
    class TActorSystem;
    class TMailboxTable;
    struct TMailboxHeader;

    class TExecutorThread;
    class IActor;
    class ISchedulerCookie;

    namespace NLog {
        struct TSettings;
    }

    struct TActorContext;

    struct TActivationContext {
    public:
        TMailboxHeader& Mailbox;
        TExecutorThread& ExecutorThread;
        const NHPTimer::STime EventStart;

    protected:
        explicit TActivationContext(TMailboxHeader& mailbox, TExecutorThread& executorThread, NHPTimer::STime eventStart)
            : Mailbox(mailbox)
            , ExecutorThread(executorThread)
            , EventStart(eventStart)
        {
        }

    public:
        static bool Send(TAutoPtr<IEventHandle> ev);

        /**
         * Schedule one-shot event that will be send at given time point in the future.
         *
         * @param deadline   the wallclock time point in future when event must be send
         * @param ev         the event to send
         * @param cookie     cookie that will be piggybacked with event
         */
        static void Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie = nullptr);

        /**
         * Schedule one-shot event that will be send at given time point in the future.
         *
         * @param deadline   the monotonic time point in future when event must be send
         * @param ev         the event to send
         * @param cookie     cookie that will be piggybacked with event
         */
        static void Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie = nullptr);

        /**
         * Schedule one-shot event that will be send after given delay.
         *
         * @param delta      the time from now to delay event sending
         * @param ev         the event to send
         * @param cookie     cookie that will be piggybacked with event
         */
        static void Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie = nullptr);

        static TInstant Now();
        static TMonotonic Monotonic();
        NLog::TSettings* LoggerSettings() const;

        // register new actor in ActorSystem on new fresh mailbox.
        static TActorId Register(IActor* actor, TActorId parentId = TActorId(), TMailboxType::EType mailboxType = TMailboxType::HTSwap, ui32 poolId = Max<ui32>());

        // Register new actor in ActorSystem on same _mailbox_ as current actor.
        // There is one thread per mailbox to execute actor, which mean
        // no _cpu core scalability_ for such actors.
        // This method of registration can be usefull if multiple actors share
        // some memory.
        static TActorId RegisterWithSameMailbox(IActor* actor, TActorId parentId);

        static const TActorContext& AsActorContext();
        static TActorContext ActorContextFor(TActorId id);

        static TActorId InterconnectProxy(ui32 nodeid);
        static TActorSystem* ActorSystem();

        static i64 GetCurrentEventTicks();
        static double GetCurrentEventTicksAsSeconds();
    };

    struct TActorContext: public TActivationContext {
        const TActorId SelfID;

        explicit TActorContext(TMailboxHeader& mailbox, TExecutorThread& executorThread, NHPTimer::STime eventStart, const TActorId& selfID)
            : TActivationContext(mailbox, executorThread, eventStart)
            , SelfID(selfID)
        {
        }

        bool Send(const TActorId& recipient, IEventBase* ev, ui32 flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) const;
        template <typename TEvent>
        bool Send(const TActorId& recipient, THolder<TEvent> ev, ui32 flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) const {
            return Send(recipient, static_cast<IEventBase*>(ev.Release()), flags, cookie, std::move(traceId));
        }
        bool Send(TAutoPtr<IEventHandle> ev) const;

        TInstant Now() const;
        TMonotonic Monotonic() const;

        /**
         * Schedule one-shot event that will be send at given time point in the future.
         *
         * @param deadline   the wallclock time point in future when event must be send
         * @param ev         the event to send
         * @param cookie     cookie that will be piggybacked with event
         */
        void Schedule(TInstant deadline, IEventBase* ev, ISchedulerCookie* cookie = nullptr) const;

        /**
         * Schedule one-shot event that will be send at given time point in the future.
         *
         * @param deadline   the monotonic time point in future when event must be send
         * @param ev         the event to send
         * @param cookie     cookie that will be piggybacked with event
         */
        void Schedule(TMonotonic deadline, IEventBase* ev, ISchedulerCookie* cookie = nullptr) const;

        /**
         * Schedule one-shot event that will be send after given delay.
         *
         * @param delta      the time from now to delay event sending
         * @param ev         the event to send
         * @param cookie     cookie that will be piggybacked with event
         */
        void Schedule(TDuration delta, IEventBase* ev, ISchedulerCookie* cookie = nullptr) const;

        TActorContext MakeFor(const TActorId& otherId) const {
            return TActorContext(Mailbox, ExecutorThread, EventStart, otherId);
        }

        // register new actor in ActorSystem on new fresh mailbox.
        TActorId Register(IActor* actor, TMailboxType::EType mailboxType = TMailboxType::HTSwap, ui32 poolId = Max<ui32>()) const;

        // Register new actor in ActorSystem on same _mailbox_ as current actor.
        // There is one thread per mailbox to execute actor, which mean
        // no _cpu core scalability_ for such actors.
        // This method of registration can be usefull if multiple actors share
        // some memory.
        TActorId RegisterWithSameMailbox(IActor* actor) const;

        std::pair<ui32, ui32> CountMailboxEvents(ui32 maxTraverse = Max<ui32>()) const;
    };

    extern Y_POD_THREAD(TActivationContext*) TlsActivationContext;

    struct TActorIdentity: public TActorId {
        explicit TActorIdentity(TActorId actorId)
            : TActorId(actorId)
        {
        }

        void operator=(TActorId actorId) {
            *this = TActorIdentity(actorId);
        }

        bool Send(const TActorId& recipient, IEventBase* ev, ui32 flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) const;
        void Schedule(TInstant deadline, IEventBase* ev, ISchedulerCookie* cookie = nullptr) const;
        void Schedule(TMonotonic deadline, IEventBase* ev, ISchedulerCookie* cookie = nullptr) const;
        void Schedule(TDuration delta, IEventBase* ev, ISchedulerCookie* cookie = nullptr) const;
    };

    class IActor;

    class IActorOps : TNonCopyable {
    public:
        virtual void Describe(IOutputStream&) const noexcept = 0;
        virtual bool Send(const TActorId& recipient, IEventBase*, ui32 flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) const noexcept = 0;

        /**
         * Schedule one-shot event that will be send at given time point in the future.
         *
         * @param deadline   the wallclock time point in future when event must be send
         * @param ev         the event to send
         * @param cookie     cookie that will be piggybacked with event
         */
        virtual void Schedule(TInstant deadline, IEventBase* ev, ISchedulerCookie* cookie = nullptr) const noexcept = 0;

        /**
         * Schedule one-shot event that will be send at given time point in the future.
         *
         * @param deadline   the monotonic time point in future when event must be send
         * @param ev         the event to send
         * @param cookie     cookie that will be piggybacked with event
         */
        virtual void Schedule(TMonotonic deadline, IEventBase* ev, ISchedulerCookie* cookie = nullptr) const noexcept = 0;

        /**
         * Schedule one-shot event that will be send after given delay.
         *
         * @param delta      the time from now to delay event sending
         * @param ev         the event to send
         * @param cookie     cookie that will be piggybacked with event
         */
        virtual void Schedule(TDuration delta, IEventBase* ev, ISchedulerCookie* cookie = nullptr) const noexcept = 0;

        virtual TActorId Register(IActor*, TMailboxType::EType mailboxType = TMailboxType::HTSwap, ui32 poolId = Max<ui32>()) const noexcept = 0;
        virtual TActorId RegisterWithSameMailbox(IActor*) const noexcept = 0;
    };

    class TDecorator;

    class IActor : protected IActorOps {
    public:
        typedef void (IActor::*TReceiveFunc)(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx);

    private:
        TReceiveFunc StateFunc;
        TActorIdentity SelfActorId;
        i64 ElapsedTicks;
        ui64 HandledEvents;

        friend void DoActorInit(TActorSystem*, IActor*, const TActorId&, const TActorId&);
        friend class TDecorator;

    public:
        /// @sa services.proto NKikimrServices::TActivity::EType
        enum EActorActivity {
            OTHER = 0,
            ACTOR_SYSTEM = 1,
            ACTORLIB_COMMON = 2,
            ACTORLIB_STATS = 3,
            LOG_ACTOR = 4,
            INTERCONNECT_PROXY_TCP = 12,
            INTERCONNECT_SESSION_TCP = 13,
            INTERCONNECT_COMMON = 171,
            SELF_PING_ACTOR = 207,
            TEST_ACTOR_RUNTIME = 283,
            INTERCONNECT_HANDSHAKE = 284,
            INTERCONNECT_POLLER = 285,
            INTERCONNECT_SESSION_KILLER = 286,
            ACTOR_SYSTEM_SCHEDULER_ACTOR = 312,
            ACTOR_FUTURE_CALLBACK = 337,
            INTERCONNECT_MONACTOR = 362,
            INTERCONNECT_LOAD_ACTOR = 376,
            INTERCONNECT_LOAD_RESPONDER = 377,
            NAMESERVICE = 450,
            DNS_RESOLVER = 481,
            INTERCONNECT_PROXY_WRAPPER = 546,
        };

        using EActivityType = EActorActivity;
        ui32 ActivityType;

    protected:
        IActor(TReceiveFunc stateFunc, ui32 activityType = OTHER)
            : StateFunc(stateFunc)
            , SelfActorId(TActorId())
            , ElapsedTicks(0)
            , HandledEvents(0)
            , ActivityType(activityType)
        {
        }

    public:
        virtual ~IActor() {
        } // must not be called for registered actors, see Die method instead

    protected:
        virtual void Die(const TActorContext& ctx); // would unregister actor so call exactly once and only from inside of message processing
        virtual void PassAway();

    public:
        template <typename T>
        void Become(T stateFunc) {
            StateFunc = static_cast<TReceiveFunc>(stateFunc);
        }

        template <typename T, typename... TArgs>
        void Become(T stateFunc, const TActorContext& ctx, TArgs&&... args) {
            StateFunc = static_cast<TReceiveFunc>(stateFunc);
            ctx.Schedule(std::forward<TArgs>(args)...);
        }

        template <typename T, typename... TArgs>
        void Become(T stateFunc, TArgs&&... args) {
            StateFunc = static_cast<TReceiveFunc>(stateFunc);
            Schedule(std::forward<TArgs>(args)...);
        }

    protected:
        void SetActivityType(ui32 activityType) {
            ActivityType = activityType;
        }

    public:
        TReceiveFunc CurrentStateFunc() const {
            return StateFunc;
        }

        // NOTE: exceptions must not escape state function but if an exception hasn't be caught
        // by the actor then we want to crash an see the stack
        void Receive(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
            (this->*StateFunc)(ev, ctx);
            HandledEvents++;
        }

        // must be called to wrap any call trasitions from one actor to another
        template<typename TActor, typename TMethod, typename... TArgs>
        static decltype((std::declval<TActor>().*std::declval<TMethod>())(std::declval<TArgs>()...))
                InvokeOtherActor(TActor& actor, TMethod&& method, TArgs&&... args) {
            struct TRecurseContext : TActorContext {
                TActivationContext *Prev;
                TRecurseContext(const TActorId& actorId)
                    : TActorContext(TActivationContext::ActorContextFor(actorId))
                    , Prev(TlsActivationContext)
                {
                    TlsActivationContext = this;
                }
                ~TRecurseContext() {
                    TlsActivationContext = Prev;
                }
            } context(actor.SelfId());
            return (actor.*method)(std::forward<TArgs>(args)...);
        }

        virtual void Registered(TActorSystem* sys, const TActorId& owner);

        virtual TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) {
            Y_UNUSED(self);
            Y_UNUSED(parentId);
            return TAutoPtr<IEventHandle>();
        }

        i64 GetElapsedTicks() const {
            return ElapsedTicks;
        }
        double GetElapsedTicksAsSeconds() const;
        void AddElapsedTicks(i64 ticks) {
            ElapsedTicks += ticks;
        }
        auto GetActivityType() const {
            return ActivityType;
        }
        ui64 GetHandledEvents() const {
            return HandledEvents;
        }
        TActorIdentity SelfId() const {
            return SelfActorId;
        }

    protected:
        void Describe(IOutputStream&) const noexcept override;
        bool Send(const TActorId& recipient, IEventBase* ev, ui32 flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) const noexcept final;
        template <typename TEvent>
        bool Send(const TActorId& recipient, THolder<TEvent> ev, ui32 flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) const{
            return Send(recipient, static_cast<IEventBase*>(ev.Release()), flags, cookie, std::move(traceId));
        }

        template <class TEvent, class ... TEventArgs>
        bool Send(TActorId recipient, TEventArgs&& ... args) const {
            return Send(recipient, MakeHolder<TEvent>(std::forward<TEventArgs>(args)...));
        }

        void Schedule(TInstant deadline, IEventBase* ev, ISchedulerCookie* cookie = nullptr) const noexcept final;
        void Schedule(TMonotonic deadline, IEventBase* ev, ISchedulerCookie* cookie = nullptr) const noexcept final;
        void Schedule(TDuration delta, IEventBase* ev, ISchedulerCookie* cookie = nullptr) const noexcept final;

        // register new actor in ActorSystem on new fresh mailbox.
        TActorId Register(IActor* actor, TMailboxType::EType mailboxType = TMailboxType::HTSwap, ui32 poolId = Max<ui32>()) const noexcept final;

        // Register new actor in ActorSystem on same _mailbox_ as current actor.
        // There is one thread per mailbox to execute actor, which mean
        // no _cpu core scalability_ for such actors.
        // This method of registration can be usefull if multiple actors share
        // some memory.
        TActorId RegisterWithSameMailbox(IActor* actor) const noexcept final;

        std::pair<ui32, ui32> CountMailboxEvents(ui32 maxTraverse = Max<ui32>()) const;

    private:
        void ChangeSelfId(TActorId actorId) {
            SelfActorId = actorId;
        }
    };

    struct TActorActivityTag {}; 
 
    inline size_t GetActivityTypeCount() { 
        return TLocalProcessKeyState<TActorActivityTag>::GetInstance().GetCount(); 
    } 
 
    inline TStringBuf GetActivityTypeName(size_t index) { 
        return TLocalProcessKeyState<TActorActivityTag>::GetInstance().GetNameByIndex(index); 
    } 
 
    template <typename TDerived>
    class TActor: public IActor {
    private:
        template <typename T, typename = const char*>
        struct HasActorName: std::false_type { };
        template <typename T>
        struct HasActorName<T, decltype((void)T::ActorName, (const char*)nullptr)>: std::true_type { };

        static ui32 GetActivityTypeIndex() {
            if constexpr(HasActorName<TDerived>::value) {
                return TLocalProcessKey<TActorActivityTag, TDerived::ActorName>::GetIndex();
            } else {
                using TActorActivity = decltype(((TDerived*)nullptr)->ActorActivityType());
                // if constexpr(std::is_enum<TActorActivity>::value) {
                    return TEnumProcessKey<TActorActivityTag, TActorActivity>::GetIndex(
                    TDerived::ActorActivityType());
                //} else {
                    // for int, ui32, ...
                //    return TEnumProcessKey<TActorActivityTag, IActor::EActorActivity>::GetIndex(
                //        static_cast<IActor::EActorActivity>(TDerived::ActorActivityType()));
                //}
            }
        } 
 
    protected:
        //* Comment this function to find unmarked activities
        static constexpr IActor::EActivityType ActorActivityType() {
            return EActorActivity::OTHER;
        } //*/

        // static constexpr char ActorName[] = "UNNAMED";

        TActor(void (TDerived::*func)(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx), ui32 activityType = GetActivityTypeIndex())
            : IActor(static_cast<TReceiveFunc>(func), activityType)
        { }

    public:
        typedef TDerived TThis;
    };


#define STFUNC_SIG TAutoPtr< ::NActors::IEventHandle>&ev, const ::NActors::TActorContext &ctx
#define STATEFN_SIG TAutoPtr<::NActors::IEventHandle>& ev
#define STFUNC(funcName) void funcName(TAutoPtr< ::NActors::IEventHandle>& ev, const ::NActors::TActorContext& ctx)
#define STATEFN(funcName) void funcName(TAutoPtr< ::NActors::IEventHandle>& ev, const ::NActors::TActorContext& )

#define STRICT_STFUNC(NAME, HANDLERS)                                                               \
    void NAME(STFUNC_SIG) {                                                                         \
        Y_UNUSED(ctx);                                                                              \
        switch (const ui32 etype = ev->GetTypeRewrite()) {                                          \
            HANDLERS                                                                                \
            default:                                                                                \
                Y_VERIFY_DEBUG(false, "%s: unexpected message type 0x%08" PRIx32, __func__, etype); \
        }                                                                                           \
    }

    inline const TActorContext& TActivationContext::AsActorContext() {
        TActivationContext* tls = TlsActivationContext;
        return *static_cast<TActorContext*>(tls);
    }

    inline TActorContext TActivationContext::ActorContextFor(TActorId id) {
        auto& tls = *TlsActivationContext;
        return TActorContext(tls.Mailbox, tls.ExecutorThread, tls.EventStart, id);
    }

    class TDecorator : public IActor {
    protected:
        THolder<IActor> Actor;

    public:
        TDecorator(THolder<IActor>&& actor)
            : IActor(static_cast<TReceiveFunc>(&TDecorator::State), actor->GetActivityType())
            , Actor(std::move(actor))
        {
        }

        void Registered(TActorSystem* sys, const TActorId& owner) override {
            Actor->ChangeSelfId(SelfId());
            Actor->Registered(sys, owner);
        }

        virtual bool DoBeforeReceiving(TAutoPtr<IEventHandle>& /*ev*/, const TActorContext& /*ctx*/) {
            return true;
        }

        virtual void DoAfterReceiving(const TActorContext& /*ctx*/)
        {
        }

        STFUNC(State) {
            if (DoBeforeReceiving(ev, ctx)) {
                Actor->Receive(ev, ctx);
                DoAfterReceiving(ctx);
            }
        }
    };

    // TTestDecorator doesn't work with the real actor system
    struct TTestDecorator : public TDecorator {
        TTestDecorator(THolder<IActor>&& actor)
            : TDecorator(std::move(actor))
        {
        }

        virtual ~TTestDecorator() = default;

        // This method must be called in the test actor system
        bool BeforeSending(TAutoPtr<IEventHandle>& ev)
        {
            bool send = true;
            TTestDecorator *decorator = dynamic_cast<TTestDecorator*>(Actor.Get());
            if (decorator) {
                send = decorator->BeforeSending(ev);
            }
            return send && ev && DoBeforeSending(ev);
        }

        virtual bool DoBeforeSending(TAutoPtr<IEventHandle>& /*ev*/) {
            return true;
        }
    };
}

template <>
inline void Out<NActors::TActorIdentity>(IOutputStream& o, const NActors::TActorIdentity& x) {
    return x.Out(o);
}

template <>
struct THash<NActors::TActorIdentity> {
    inline ui64 operator()(const NActors::TActorIdentity& x) const {
        return x.Hash();
    }
};
