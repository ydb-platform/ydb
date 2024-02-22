#pragma once

#include "actorsystem.h"
#include "event.h"
#include "executor_thread.h"
#include "monotonic.h"
#include "thread_context.h"

#include <ydb/library/actors/actor_type/indexes.h>
#include <ydb/library/actors/util/local_process_key.h>

#include <util/system/tls.h>
#include <util/generic/noncopyable.h>

namespace NActors {
    class TActorSystem;
    class TMailboxTable;
    struct TMailboxHeader;

    class TGenericExecutorThread;
    class IActor;
    class ISchedulerCookie;
    class IExecutorPool;

    namespace NLog {
        struct TSettings;
    }

    struct TActorContext;
    struct TActivationContext;

    class TActivationContextHolder {
        static thread_local TActivationContext *Value;

    public:
        [[gnu::noinline]] operator bool() const;
        [[gnu::noinline]] operator TActivationContext*() const;
        [[gnu::noinline]] TActivationContext *operator ->();
        [[gnu::noinline]] TActivationContext& operator *();
        [[gnu::noinline]] TActivationContextHolder& operator=(TActivationContext *context);
    };

    extern TActivationContextHolder TlsActivationContext;

    struct TActivationContext {
    public:
        TMailboxHeader& Mailbox;
        TGenericExecutorThread& ExecutorThread;
        const NHPTimer::STime EventStart;

    protected:
        explicit TActivationContext(TMailboxHeader& mailbox, TGenericExecutorThread& executorThread, NHPTimer::STime eventStart)
            : Mailbox(mailbox)
            , ExecutorThread(executorThread)
            , EventStart(eventStart)
        {
        }

    public:
        template <ESendingType SendingType = ESendingType::Common>
        static bool Send(TAutoPtr<IEventHandle> ev);

        template <ESendingType SendingType = ESendingType::Common>
        static bool Send(std::unique_ptr<IEventHandle> &&ev);

        template <ESendingType SendingType = ESendingType::Common>
        static bool Forward(TAutoPtr<IEventHandle>& ev, const TActorId& recipient);

        template <ESendingType SendingType = ESendingType::Common>
        static bool Forward(THolder<IEventHandle>& ev, const TActorId& recipient);

        /**
         * Schedule one-shot event that will be send at given time point in the future.
         *
         * @param deadline   the wallclock time point in future when event must be send
         * @param ev         the event to send
         * @param cookie     cookie that will be piggybacked with event
         */
        static void Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie = nullptr);
        static void Schedule(TInstant deadline, std::unique_ptr<IEventHandle> ev, ISchedulerCookie* cookie = nullptr) {
            return Schedule(deadline, TAutoPtr<IEventHandle>(ev.release()), cookie);
        }

        /**
         * Schedule one-shot event that will be send at given time point in the future.
         *
         * @param deadline   the monotonic time point in future when event must be send
         * @param ev         the event to send
         * @param cookie     cookie that will be piggybacked with event
         */
        static void Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie = nullptr);
        static void Schedule(TMonotonic deadline, std::unique_ptr<IEventHandle> ev, ISchedulerCookie* cookie = nullptr) {
            return Schedule(deadline, TAutoPtr<IEventHandle>(ev.release()), cookie);
        }

        /**
         * Schedule one-shot event that will be send after given delay.
         *
         * @param delta      the time from now to delay event sending
         * @param ev         the event to send
         * @param cookie     cookie that will be piggybacked with event
         */
        static void Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie = nullptr);
        static void Schedule(TDuration delta, std::unique_ptr<IEventHandle> ev, ISchedulerCookie* cookie = nullptr) {
            return Schedule(delta, TAutoPtr<IEventHandle>(ev.release()), cookie);
        }

        static TInstant Now();
        static TMonotonic Monotonic();
        NLog::TSettings* LoggerSettings() const;

        // register new actor in ActorSystem on new fresh mailbox.
        template <ESendingType SendingType = ESendingType::Common>
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
        using TEventFlags = IEventHandle::TEventFlags;
        explicit TActorContext(TMailboxHeader& mailbox, TGenericExecutorThread& executorThread, NHPTimer::STime eventStart, const TActorId& selfID)
            : TActivationContext(mailbox, executorThread, eventStart)
            , SelfID(selfID)
        {
        }

        template <ESendingType SendingType = ESendingType::Common>
        bool Send(const TActorId& recipient, IEventBase* ev, TEventFlags flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) const;
        template <ESendingType SendingType = ESendingType::Common>
        bool Send(const TActorId& recipient, THolder<IEventBase> ev, TEventFlags flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) const {
            return Send<SendingType>(recipient, ev.Release(), flags, cookie, std::move(traceId));
        }
        template <ESendingType SendingType = ESendingType::Common>
        bool Send(const TActorId& recipient, std::unique_ptr<IEventBase> ev, TEventFlags flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) const {
            return Send<SendingType>(recipient, ev.release(), flags, cookie, std::move(traceId));
        }
        template <ESendingType SendingType = ESendingType::Common>
        bool Send(TAutoPtr<IEventHandle> ev) const;
        template <ESendingType SendingType = ESendingType::Common>
        bool Send(std::unique_ptr<IEventHandle> &&ev) const {
            return Send<SendingType>(TAutoPtr<IEventHandle>(ev.release()));
        }
        template <ESendingType SendingType = ESendingType::Common>
        bool Forward(TAutoPtr<IEventHandle>& ev, const TActorId& recipient) const;
        template <ESendingType SendingType = ESendingType::Common>
        bool Forward(THolder<IEventHandle>& ev, const TActorId& recipient) const;

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
        template <ESendingType SendingType = ESendingType::Common>
        TActorId Register(IActor* actor, TMailboxType::EType mailboxType = TMailboxType::HTSwap, ui32 poolId = Max<ui32>()) const;

        // Register new actor in ActorSystem on same _mailbox_ as current actor.
        // There is one thread per mailbox to execute actor, which mean
        // no _cpu core scalability_ for such actors.
        // This method of registration can be usefull if multiple actors share
        // some memory.
        TActorId RegisterWithSameMailbox(IActor* actor) const;

        std::pair<ui32, ui32> CountMailboxEvents(ui32 maxTraverse = Max<ui32>()) const;
    };

    struct TActorIdentity: public TActorId {
        using TEventFlags = IEventHandle::TEventFlags;
        explicit TActorIdentity(TActorId actorId)
            : TActorId(actorId)
        {
        }

        void operator=(TActorId actorId) {
            *this = TActorIdentity(actorId);
        }

        template <ESendingType SendingType = ESendingType::Common>
        bool Send(const TActorId& recipient, IEventBase* ev, TEventFlags flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) const;
        bool SendWithContinuousExecution(const TActorId& recipient, IEventBase* ev, TEventFlags flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) const;
        void Schedule(TInstant deadline, IEventBase* ev, ISchedulerCookie* cookie = nullptr) const;
        void Schedule(TMonotonic deadline, IEventBase* ev, ISchedulerCookie* cookie = nullptr) const;
        void Schedule(TDuration delta, IEventBase* ev, ISchedulerCookie* cookie = nullptr) const;
    };

    class IActor;

    class IActorOps : TNonCopyable {
    public:
        virtual void Describe(IOutputStream&) const noexcept = 0;
        virtual bool Send(const TActorId& recipient, IEventBase*, IEventHandle::TEventFlags flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) const noexcept = 0;

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

    class TActorVirtualBehaviour {
    public:
        static void Receive(IActor* actor, std::unique_ptr<IEventHandle> ev);
    public:
    };

    class TActorCallbackBehaviour {
    private:
        using TBase = IActor;
        friend class TDecorator;
    public:
        using TReceiveFunc = void (IActor::*)(TAutoPtr<IEventHandle>& ev);
    private:
        TReceiveFunc StateFunc = nullptr;
    public:
        TActorCallbackBehaviour() = default;
        TActorCallbackBehaviour(TReceiveFunc stateFunc)
            : StateFunc(stateFunc) {
        }
        bool Initialized() const {
            return !!StateFunc;
        }

        // NOTE: exceptions must not escape state function but if an exception hasn't be caught
        // by the actor then we want to crash an see the stack
        void Receive(IActor* actor, TAutoPtr<IEventHandle>& ev);

        template <typename T>
        void Become(T stateFunc) {
            StateFunc = static_cast<TReceiveFunc>(stateFunc);
        }

        template <typename T, typename... TArgs>
        void Become(T stateFunc, const TActorContext& ctx, TArgs&&... args) {
            StateFunc = static_cast<TReceiveFunc>(stateFunc);
            ctx.Schedule(std::forward<TArgs>(args)...);
        }

        TReceiveFunc CurrentStateFunc() const {
            return StateFunc;
        }

    };

    template<bool>
    struct TActorUsageImpl {
        void OnEnqueueEvent(ui64 /*time*/) {} // called asynchronously when event is put in the mailbox
        void OnDequeueEvent() {} // called when processed by Executor
        double GetUsage(ui64 /*time*/) { return 0; } // called from collector thread
        void DoActorInit() {}
    };

    template<>
    struct TActorUsageImpl<true> {
        static constexpr int TimestampBits = 40;
        static constexpr int CountBits = 24;
        static constexpr ui64 TimestampMask = ((ui64)1 << TimestampBits) - 1;
        static constexpr ui64 CountMask = ((ui64)1 << CountBits) - 1;

        std::atomic_uint64_t QueueSizeAndTimestamp = 0;
        std::atomic_uint64_t UsedTime = 0; // how much time did we consume since last GetUsage() call
        ui64 LastUsageTimestamp = 0; // when GetUsage() was called the last time

        void OnEnqueueEvent(ui64 time);
        void OnDequeueEvent();
        double GetUsage(ui64 time);
        void DoActorInit() { LastUsageTimestamp = GetCycleCountFast(); }
    };

    class IActor
        : protected IActorOps
        , public TActorUsageImpl<ActorLibCollectUsageStats>
    {
    private:
        TActorIdentity SelfActorId;
        i64 ElapsedTicks;
        friend void DoActorInit(TActorSystem*, IActor*, const TActorId&, const TActorId&);
        friend class TDecorator;

    private: // stuck actor monitoring
        TMonotonic LastReceiveTimestamp;
        size_t StuckIndex = Max<size_t>();
        friend class TExecutorPoolBaseMailboxed;
        friend class TGenericExecutorThread;

        IActor(const ui32 activityType)
            : SelfActorId(TActorId())
            , ElapsedTicks(0)
            , ActivityType(activityType)
            , HandledEvents(0) {
        }

    protected:
        TActorCallbackBehaviour CImpl;
    public:
        using TEventFlags = IEventHandle::TEventFlags;
        using TReceiveFunc = TActorCallbackBehaviour::TReceiveFunc;
        /// @sa services.proto NKikimrServices::TActivity::EType
        using EActorActivity = EInternalActorType;
        using EActivityType = EActorActivity;
        ui32 ActivityType;

    protected:
        ui64 HandledEvents;

        template <typename EEnum = EActivityType, typename std::enable_if<std::is_enum<EEnum>::value, bool>::type v = true>
        IActor(const EEnum activityEnumType = EActivityType::OTHER)
            : IActor(TEnumProcessKey<TActorActivityTag, EEnum>::GetIndex(activityEnumType)) {
        }

        IActor(TActorCallbackBehaviour&& cImpl, const ui32 activityType)
            : SelfActorId(TActorId())
            , ElapsedTicks(0)
            , CImpl(std::move(cImpl))
            , ActivityType(activityType)
            , HandledEvents(0)
        {
        }

        template <typename EEnum = EActivityType, typename std::enable_if<std::is_enum<EEnum>::value, bool>::type v = true>
        IActor(TActorCallbackBehaviour&& cImpl, const EEnum activityEnumType = EActivityType::OTHER)
            : IActor(std::move(cImpl), TEnumProcessKey<TActorActivityTag, EEnum>::GetIndex(activityEnumType)) {
        }

    public:
        template <class TEventBase>
        class TEventSenderFromActor: ::TNonCopyable {
        private:
            TEventFlags Flags = 0;
            ui64 Cookie = 0;
            const TActorIdentity SenderId;
            NWilson::TTraceId TraceId = {};
            std::unique_ptr<TEventBase> Event;
        public:
            template <class... Types>
            TEventSenderFromActor(const IActor* owner, Types&&... args)
                : SenderId(owner->SelfId())
                , Event(new TEventBase(std::forward<Types>(args)...)) {

            }

            TEventSenderFromActor& SetFlags(const TEventFlags flags) {
                Flags = flags;
                return *this;
            }

            TEventSenderFromActor& SetCookie(const ui64 value) {
                Cookie = value;
                return *this;
            }

            TEventSenderFromActor& SetTraceId(NWilson::TTraceId&& value) {
                TraceId = std::move(value);
                return *this;
            }

            bool SendTo(const TActorId& recipient) {
                return SenderId.Send(recipient, Event.release(), Flags, Cookie, std::move(TraceId));
            }
        };

        template <class TEvent, class... Types>
        TEventSenderFromActor<TEvent> Sender(Types&&... args) const {
            return TEventSenderFromActor<TEvent>(this, std::forward<Types>(args)...);
        }

        virtual ~IActor() {
        } // must not be called for registered actors, see Die method instead

    protected:
        virtual void Die(const TActorContext& ctx); // would unregister actor so call exactly once and only from inside of message processing
        virtual void PassAway();

    protected:
        void SetActivityType(ui32 activityType) {
            ActivityType = activityType;
        }

    public:
        class TPassAwayGuard: TMoveOnly {
        private:
            IActor* Owner = nullptr;
        public:
            TPassAwayGuard(TPassAwayGuard&& item) {
                Owner = item.Owner;
                item.Owner = nullptr;
            }

            TPassAwayGuard(IActor* owner)
                : Owner(owner)
            {

            }

            ~TPassAwayGuard() {
                if (Owner) {
                    Owner->PassAway();
                }
            }
        };

        TPassAwayGuard PassAwayGuard() {
            return TPassAwayGuard(this);
        }

        // must be called to wrap any call trasitions from one actor to another
        template<typename TActor, typename TMethod, typename... TArgs>
        static std::invoke_result_t<TMethod, TActor, TArgs...> InvokeOtherActor(TActor& actor, TMethod&& method, TArgs&&... args) {
            struct TRecurseContext : TActorContext {
                TActivationContext* const Prev;

                TRecurseContext(const TActorId& actorId)
                    : TActorContext(TActivationContext::ActorContextFor(actorId))
                    , Prev(TlsActivationContext)
                {
                    TlsActivationContext = this;
                }

                ~TRecurseContext() {
                    Y_ABORT_UNLESS(TlsActivationContext == this, "TlsActivationContext mismatch; probably InvokeOtherActor was invoked from a coroutine");
                    TlsActivationContext = Prev;
                }
            } context(actor.SelfId());

            return std::invoke(std::forward<TMethod>(method), actor, std::forward<TArgs>(args)...);
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
        ui32 GetActivityType() const {
            return ActivityType;
        }
        ui64 GetHandledEvents() const {
            return HandledEvents;
        }
        TActorIdentity SelfId() const {
            return SelfActorId;
        }

        void Receive(TAutoPtr<IEventHandle>& ev) {
            ++HandledEvents;
            LastReceiveTimestamp = TActivationContext::Monotonic();
            if (CImpl.Initialized()) {
                CImpl.Receive(this, ev);
            } else {
                TActorVirtualBehaviour::Receive(this, std::unique_ptr<IEventHandle>(ev.Release()));
            }
        }

        TActorContext ActorContext() const {
            return TActivationContext::ActorContextFor(SelfId());
        }

    protected:
        void SetEnoughCpu(bool isEnough) {
            if (TlsThreadContext) {
                TlsThreadContext->IsEnoughCpu = isEnough;
            }
        }

        void Describe(IOutputStream&) const noexcept override;
        bool Send(TAutoPtr<IEventHandle> ev) const noexcept;
        bool Send(const TActorId& recipient, IEventBase* ev, TEventFlags flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) const noexcept final;
        bool Send(const TActorId& recipient, THolder<IEventBase> ev, TEventFlags flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) const{
            return Send(recipient, ev.Release(), flags, cookie, std::move(traceId));
        }
        bool Send(const TActorId& recipient, std::unique_ptr<IEventBase> ev, TEventFlags flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) const {
            return Send(recipient, ev.release(), flags, cookie, std::move(traceId));
        }

        template <class TEvent, class ... TEventArgs>
        bool Send(TActorId recipient, TEventArgs&& ... args) const {
            return Send(recipient, MakeHolder<TEvent>(std::forward<TEventArgs>(args)...));
        }

        template <ESendingType SendingType>
        bool Send(const TActorId& recipient, IEventBase* ev, TEventFlags flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) const;
        template <ESendingType SendingType>
        bool Send(const TActorId& recipient, THolder<IEventBase> ev, TEventFlags flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) const {
            return Send(recipient, ev.Release(), flags, cookie, std::move(traceId));
        }
        template <ESendingType SendingType>
        bool Send(const TActorId& recipient, std::unique_ptr<IEventBase> ev, TEventFlags flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) const {
            return Send(recipient, ev.release(), flags, cookie, std::move(traceId));
        }

        static bool Forward(TAutoPtr<IEventHandle>& ev, const TActorId& recipient) {
            return TActivationContext::Forward(ev, recipient);
        }

        static bool Forward(THolder<IEventHandle>& ev, const TActorId& recipient) {
            return TActivationContext::Forward(ev, recipient);
        }

        template <typename TEventHandle>
        static bool Forward(TAutoPtr<TEventHandle>& ev, const TActorId& recipient) {
            TAutoPtr<IEventHandle> evi(ev.Release());
            return TActivationContext::Forward(evi, recipient);
        }

        void Schedule(TInstant deadline, IEventBase* ev, ISchedulerCookie* cookie = nullptr) const noexcept final;
        void Schedule(TMonotonic deadline, IEventBase* ev, ISchedulerCookie* cookie = nullptr) const noexcept final;
        void Schedule(TDuration delta, IEventBase* ev, ISchedulerCookie* cookie = nullptr) const noexcept final;

        // register new actor in ActorSystem on new fresh mailbox.
        TActorId Register(IActor* actor, TMailboxType::EType mailboxType = TMailboxType::HTSwap, ui32 poolId = Max<ui32>()) const noexcept final;

        template <ESendingType SendingType>
        TActorId Register(IActor* actor, TMailboxType::EType mailboxType = TMailboxType::HTSwap, ui32 poolId = Max<ui32>()) const noexcept;

        // Register new actor in ActorSystem on same _mailbox_ as current actor.
        // There is one thread per mailbox to execute actor, which mean
        // no _cpu core scalability_ for such actors.
        // This method of registration can be useful if multiple actors share
        // some memory.
        TActorId RegisterWithSameMailbox(IActor* actor) const noexcept final;

        std::pair<ui32, ui32> CountMailboxEvents(ui32 maxTraverse = Max<ui32>()) const;

    private:
        void ChangeSelfId(TActorId actorId) {
            SelfActorId = actorId;
        }
    };

    inline size_t GetActivityTypeCount() {
        return TLocalProcessKeyState<TActorActivityTag>::GetInstance().GetCount();
    }

    inline TStringBuf GetActivityTypeName(size_t index) {
        return TLocalProcessKeyState<TActorActivityTag>::GetInstance().GetNameByIndex(index);
    }

    class IActorCallback: public IActor {
    protected:
        template <class TEnum = IActor::EActivityType>
        IActorCallback(TReceiveFunc stateFunc, const TEnum activityType = IActor::EActivityType::OTHER)
            : IActor(TActorCallbackBehaviour(stateFunc), activityType) {

        }

        IActorCallback(TReceiveFunc stateFunc, const ui32 activityType)
            : IActor(TActorCallbackBehaviour(stateFunc), activityType) {

        }

    public:
        template <typename T>
        void Become(T stateFunc) {
            CImpl.Become(stateFunc);
        }

        template <typename T, typename... TArgs>
        void Become(T stateFunc, const TActorContext& ctx, TArgs&&... args) {
            CImpl.Become(stateFunc, ctx, std::forward<TArgs>(args)...);
        }

        template <typename T, typename... TArgs>
        void Become(T stateFunc, TArgs&&... args) {
            CImpl.Become(stateFunc);
            Schedule(std::forward<TArgs>(args)...);
        }

        TReceiveFunc CurrentStateFunc() const {
            return CImpl.CurrentStateFunc();
        }
    };

    template <typename TDerived>
    class TActor: public IActorCallback {
    private:
        using TDerivedReceiveFunc = void (TDerived::*)(TAutoPtr<IEventHandle>& ev);

        template <typename T, typename = const char*>
        struct HasActorName: std::false_type {};
        template <typename T>
        struct HasActorName<T, decltype((void)T::ActorName, (const char*)nullptr)>: std::true_type {};

        template <typename T, typename = const char*>
        struct HasActorActivityType: std::false_type {};
        template <typename T>
        struct HasActorActivityType<T, decltype((void)T::ActorActivityType, (const char*)nullptr)>: std::true_type {};

        static ui32 GetActivityTypeIndexImpl() {
            if constexpr(HasActorName<TDerived>::value) {
                return TLocalProcessKey<TActorActivityTag, TDerived::ActorName>::GetIndex();
            } else if constexpr (HasActorActivityType<TDerived>::value) {
                using TActorActivity = decltype(((TDerived*)nullptr)->ActorActivityType());
                static_assert(std::is_enum<TActorActivity>::value);
                return TEnumProcessKey<TActorActivityTag, TActorActivity>::GetIndex(TDerived::ActorActivityType());
            } else {
                // 200 characters is limit for solomon metric tag length
                return TLocalProcessExtKey<TActorActivityTag, TDerived, 200>::GetIndex();
            }
        }

        static ui32 GetActivityTypeIndex() {
            static const ui32 result = GetActivityTypeIndexImpl();
            return result;
        }

    protected:
        // static constexpr char ActorName[] = "UNNAMED";

        TActor(TDerivedReceiveFunc func)
            : IActorCallback(static_cast<TReceiveFunc>(func), GetActivityTypeIndex()) {
        }

        template <class TEnum = EActivityType>
        TActor(TDerivedReceiveFunc func, const TEnum activityEnumType = EActivityType::OTHER)
            : IActorCallback(static_cast<TReceiveFunc>(func), activityEnumType) {
        }

        TActor(TDerivedReceiveFunc func, const TString& actorName)
            : IActorCallback(static_cast<TReceiveFunc>(func), TLocalProcessKeyState<TActorActivityTag>::GetInstance().Register(actorName)) {
        }

    public:
        typedef TDerived TThis;

        // UnsafeBecome methods don't verify the bindings of the stateFunc to the TDerived
        template <typename T>
        void UnsafeBecome(T stateFunc) {
            this->IActorCallback::Become(stateFunc);
        }

        template <typename T, typename... TArgs>
        void UnsafeBecome(T stateFunc, const TActorContext& ctx, TArgs&&... args) {
            this->IActorCallback::Become(stateFunc, ctx, std::forward<TArgs>(args)...);
        }

        template <typename T, typename... TArgs>
        void UnsafeBecome(T stateFunc, TArgs&&... args) {
            this->IActorCallback::Become(stateFunc, std::forward<TArgs>(args)...);
        }

        template <typename T>
        void Become(T stateFunc) {
            // TODO(kruall): have to uncomment asserts after end of sync contrib/ydb
            // static_assert(std::is_convertible_v<T, TDerivedReceiveFunc>);
            this->IActorCallback::Become(stateFunc);
        }

        template <typename T, typename... TArgs>
        void Become(T stateFunc, const TActorContext& ctx, TArgs&&... args) {
            // static_assert(std::is_convertible_v<T, TDerivedReceiveFunc>);
            this->IActorCallback::Become(stateFunc, ctx, std::forward<TArgs>(args)...);
        }

        template <typename T, typename... TArgs>
        void Become(T stateFunc, TArgs&&... args) {
            // static_assert(std::is_convertible_v<T, TDerivedReceiveFunc>);
            this->IActorCallback::Become(stateFunc, std::forward<TArgs>(args)...);
        }
    };


#define STFUNC_SIG TAutoPtr<::NActors::IEventHandle>& ev
#define STATEFN_SIG TAutoPtr<::NActors::IEventHandle>& ev
#define STFUNC(funcName) void funcName(TAutoPtr<::NActors::IEventHandle>& ev)
#define STATEFN(funcName) void funcName(TAutoPtr<::NActors::IEventHandle>& ev)

#define STFUNC_STRICT_UNHANDLED_MSG_HANDLER Y_DEBUG_ABORT_UNLESS(false, "%s: unexpected message type 0x%08" PRIx32, __func__, etype);

#define STFUNC_BODY(HANDLERS, UNHANDLED_MSG_HANDLER)                    \
    switch (const ui32 etype = ev->GetTypeRewrite()) {                  \
        HANDLERS                                                        \
    default:                                                            \
        UNHANDLED_MSG_HANDLER                                           \
    }

#define STRICT_STFUNC_BODY(HANDLERS) STFUNC_BODY(HANDLERS, STFUNC_STRICT_UNHANDLED_MSG_HANDLER)

#define STRICT_STFUNC(NAME, HANDLERS)           \
    void NAME(STFUNC_SIG) {                     \
        STRICT_STFUNC_BODY(HANDLERS)            \
    }

#define STRICT_STFUNC_EXC(NAME, HANDLERS, EXCEPTION_HANDLERS)           \
    void NAME(STFUNC_SIG) {                                             \
        try {                                                           \
            STRICT_STFUNC_BODY(HANDLERS)                                \
        }                                                               \
        EXCEPTION_HANDLERS                                              \
    }

    inline const TActorContext& TActivationContext::AsActorContext() {
        TActivationContext* tls = TlsActivationContext;
        return *static_cast<TActorContext*>(tls);
    }

    inline TActorContext TActivationContext::ActorContextFor(TActorId id) {
        auto& tls = *TlsActivationContext;
        return TActorContext(tls.Mailbox, tls.ExecutorThread, tls.EventStart, id);
    }

    class TDecorator : public IActorCallback {
    protected:
        THolder<IActor> Actor;

    public:
        TDecorator(THolder<IActor>&& actor)
            : IActorCallback(static_cast<TReceiveFunc>(&TDecorator::State), actor->GetActivityType())
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
            auto ctx(ActorContext());
            if (DoBeforeReceiving(ev, ctx)) {
                Actor->Receive(ev);
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


    template <ESendingType SendingType>
    bool TGenericExecutorThread::Send(TAutoPtr<IEventHandle> ev) {
#ifdef USE_ACTOR_CALLSTACK
        do {
            (ev)->Callstack = TCallstack::GetTlsCallstack();
            (ev)->Callstack.Trace();
        } while (false)
#endif
        Ctx.IncrementSentEvents();
        return ActorSystem->Send<SendingType>(ev);
    }

    template <ESendingType SendingType>
    TActorId TGenericExecutorThread::RegisterActor(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId,
            TActorId parentId)
    {
        if (!parentId) {
            parentId = CurrentRecipient;
        }
        if (poolId == Max<ui32>()) {
            if constexpr (SendingType == ESendingType::Common) {
                return Ctx.Executor->Register(actor, mailboxType, ++RevolvingWriteCounter, parentId);
            } else if (!TlsThreadContext) {
                return Ctx.Executor->Register(actor, mailboxType, ++RevolvingWriteCounter, parentId);
            } else {
                ESendingType previousType = std::exchange(TlsThreadContext->SendingType, SendingType);
                TActorId id = Ctx.Executor->Register(actor, mailboxType, ++RevolvingWriteCounter, parentId);
                TlsThreadContext->SendingType = previousType;
                return id;
            }
        } else {
            return ActorSystem->Register<SendingType>(actor, mailboxType, poolId, ++RevolvingWriteCounter, parentId);
        }
    }

    template <ESendingType SendingType>
    TActorId TGenericExecutorThread::RegisterActor(IActor* actor, TMailboxHeader* mailbox, ui32 hint, TActorId parentId) {
        if (!parentId) {
            parentId = CurrentRecipient;
        }
        if constexpr (SendingType == ESendingType::Common) {
            return Ctx.Executor->Register(actor, mailbox, hint, parentId);
        } else if (!TlsActivationContext) {
            return Ctx.Executor->Register(actor, mailbox, hint, parentId);
        } else {
            ESendingType previousType = std::exchange(TlsThreadContext->SendingType, SendingType);
            TActorId id = Ctx.Executor->Register(actor, mailbox, hint, parentId);
            TlsThreadContext->SendingType = previousType;
            return id;
        }
    }


    template <ESendingType SendingType>
    bool TActivationContext::Send(TAutoPtr<IEventHandle> ev) {
        return TlsActivationContext->ExecutorThread.Send<SendingType>(ev);
    }

    template <ESendingType SendingType>
    bool TActivationContext::Send(std::unique_ptr<IEventHandle> &&ev) {
        return TlsActivationContext->ExecutorThread.Send<SendingType>(ev.release());
    }

    template <ESendingType SendingType>
    bool TActivationContext::Forward(TAutoPtr<IEventHandle>& ev, const TActorId& recipient) {
        return Send(IEventHandle::Forward(ev, recipient));
    }

    template <ESendingType SendingType>
    bool TActivationContext::Forward(THolder<IEventHandle>& ev, const TActorId& recipient) {
        return Send(IEventHandle::Forward(ev, recipient));
    }

    template <ESendingType SendingType>
    bool TActorContext::Send(const TActorId& recipient, IEventBase* ev, TEventFlags flags, ui64 cookie, NWilson::TTraceId traceId) const {
        return Send<SendingType>(new IEventHandle(recipient, SelfID, ev, flags, cookie, nullptr, std::move(traceId)));
    }

    template <ESendingType SendingType>
    bool TActorContext::Send(TAutoPtr<IEventHandle> ev) const {
        return ExecutorThread.Send<SendingType>(ev);
    }

    template <ESendingType SendingType>
    bool TActorContext::Forward(TAutoPtr<IEventHandle>& ev, const TActorId& recipient) const {
        return ExecutorThread.Send<SendingType>(IEventHandle::Forward(ev, recipient));
    }

    template <ESendingType SendingType>
    bool TActorContext::Forward(THolder<IEventHandle>& ev, const TActorId& recipient) const {
        return ExecutorThread.Send<SendingType>(IEventHandle::Forward(ev, recipient));
    }

    template <ESendingType SendingType>
    TActorId TActivationContext::Register(IActor* actor, TActorId parentId, TMailboxType::EType mailboxType, ui32 poolId) {
        return TlsActivationContext->ExecutorThread.RegisterActor<SendingType>(actor, mailboxType, poolId, parentId);
    }

    template <ESendingType SendingType>
    TActorId TActorContext::Register(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId) const {
        return ExecutorThread.RegisterActor<SendingType>(actor, mailboxType, poolId, SelfID);
    }

    template <ESendingType SendingType>
    bool TActorIdentity::Send(const TActorId& recipient, IEventBase* ev, TEventFlags flags, ui64 cookie, NWilson::TTraceId traceId) const {
        return TActivationContext::Send<SendingType>(new IEventHandle(recipient, *this, ev, flags, cookie, nullptr, std::move(traceId)));
    }

    template <ESendingType SendingType>
    bool IActor::Send(const TActorId& recipient, IEventBase* ev, TEventFlags flags, ui64 cookie, NWilson::TTraceId traceId) const {
        return SelfActorId.Send<SendingType>(recipient, ev, flags, cookie, std::move(traceId));
    }

    template <ESendingType SendingType>
    TActorId IActor::Register(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId) const noexcept {
        Y_ABORT_UNLESS(actor);
        return TlsActivationContext->ExecutorThread.RegisterActor<SendingType>(actor, mailboxType, poolId, SelfActorId);
    }


    template <ESendingType SendingType>
    TActorId TActorSystem::Register(IActor* actor, TMailboxType::EType mailboxType, ui32 executorPool,
                        ui64 revolvingCounter, const TActorId& parentId) {
        Y_ABORT_UNLESS(actor);
        Y_ABORT_UNLESS(executorPool < ExecutorPoolCount, "executorPool# %" PRIu32 ", ExecutorPoolCount# %" PRIu32,
                (ui32)executorPool, (ui32)ExecutorPoolCount);
        if constexpr (SendingType == ESendingType::Common) {
            return CpuManager->GetExecutorPool(executorPool)->Register(actor, mailboxType, revolvingCounter, parentId);
        } else if (!TlsThreadContext) {
            return CpuManager->GetExecutorPool(executorPool)->Register(actor, mailboxType, revolvingCounter, parentId);
        } else {
            ESendingType previousType = std::exchange(TlsThreadContext->SendingType, SendingType);
            TActorId id = CpuManager->GetExecutorPool(executorPool)->Register(actor, mailboxType, revolvingCounter, parentId);
            TlsThreadContext->SendingType = previousType;
            return id;
        }
    }

    template <ESendingType SendingType>
    bool TActorSystem::Send(TAutoPtr<IEventHandle> ev) const {
        if constexpr (SendingType == ESendingType::Common) {
            return this->GenericSend< &IExecutorPool::Send>(ev);
        } else {
            return this->SpecificSend(ev, SendingType);
        }
    }

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

template<> struct std::hash<NActors::TActorIdentity> : THash<NActors::TActorIdentity> {};
