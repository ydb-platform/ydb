#pragma once

#include "event.h"
#include "mailbox.h"
#include "monotonic.h"

#include <ydb/library/actors/actor_type/indexes.h>
#include <ydb/library/actors/util/datetime.h>
#include <ydb/library/actors/util/local_process_key.h>

#include <util/system/tls.h>
#include <util/generic/noncopyable.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_set.h>
#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>

namespace NActors {
    class TActorSystem;
    class TMailboxTable;
    class TMailbox;

    class TExecutorThread;
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
        TMailbox& Mailbox;
        TExecutorThread& ExecutorThread;
        const NHPTimer::STime EventStart;

    protected:
        explicit TActivationContext(TMailbox& mailbox, TExecutorThread& executorThread, NHPTimer::STime eventStart)
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

        static bool Send(const TActorId& recipient, std::unique_ptr<IEventBase> ev, ui32 flags = 0, ui64 cookie = 0);

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

        static void EnableMailboxStats();

        static ui32 GetOverwrittenEventsPerMailbox();
        static void SetOverwrittenEventsPerMailbox(ui32 value);
        static ui64 GetOverwrittenTimePerMailboxTs();
        static void SetOverwrittenTimePerMailboxTs(ui64 value);
    };

    struct TActorContext: public TActivationContext {
        const TActorId SelfID;
        using TEventFlags = IEventHandle::TEventFlags;
        explicit TActorContext(TMailbox& mailbox, TExecutorThread& executorThread, NHPTimer::STime eventStart, const TActorId& selfID)
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

        void Schedule(TInstant deadline, std::unique_ptr<IEventHandle> ev, ISchedulerCookie* cookie = nullptr) const;
        void Schedule(TMonotonic deadline, std::unique_ptr<IEventHandle> ev, ISchedulerCookie* cookie = nullptr) const;
        void Schedule(TDuration delta, std::unique_ptr<IEventHandle> ev, ISchedulerCookie* cookie = nullptr) const;

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
        virtual void Describe(IOutputStream&) const = 0;
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

        virtual TActorId RegisterAlias() noexcept = 0;
        virtual void UnregisterAlias(const TActorId& actorId) noexcept = 0;
    };

    class TDecorator;

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


    /**
     * Optional interface for actors with exception handling
     */
    class IActorExceptionHandler {
    protected:
        ~IActorExceptionHandler() = default;

    public:
        /**
         * Handles std::exception_ptr which allows to catch any thrown object.
         * By default ignores anything that is not an std::exception subclass.
         */
        virtual bool OnUnhandledException(const std::exception_ptr& excPtr) {
            try {
                std::rethrow_exception(excPtr);
            } catch (const std::exception& exc) {
                return OnUnhandledException(exc);
            } catch (...) {
                return false;
            }
        }

        /**
         * Called when actor's event handler throws an std::exception subclass
         *
         * The implementation is supposed to return true for handled exceptions
         * and false to rethrow (which will likely result in a process crash).
         */
        virtual bool OnUnhandledException(const std::exception&) = 0;
    };

    class TActorActivityType {
    public:
        TActorActivityType()
            : TActorActivityType(FromEnum(EInternalActorType::OTHER))
        {}

        template <typename EEnum>
        static TActorActivityType FromEnum(EEnum activityType) requires (std::is_enum_v<EEnum>) {
            return FromIndex(TEnumProcessKey<TActorActivityTag, EEnum>::GetIndex(activityType));
        }

        static TActorActivityType FromName(TStringBuf activityName) {
            return FromIndex(TLocalProcessKeyState<TActorActivityTag>::GetInstance().Register(activityName));
        }

        template <const char* Name>
        static TActorActivityType FromStaticName() {
            return FromIndex(TLocalProcessKey<TActorActivityTag, Name>::GetIndex());
        }

        template <typename T>
        static TActorActivityType FromTypeName() {
            // 200 characters is limit for solomon metric tag length
            return FromIndex(TLocalProcessExtKey<TActorActivityTag, T, 200>::GetIndex());
        }

        static constexpr TActorActivityType FromIndex(size_t index) {
            return TActorActivityType(index);
        }

        constexpr ui32 GetIndex() const {
            return Index;
        }

        TStringBuf GetName() const {
            return TLocalProcessKeyState<TActorActivityTag>::GetInstance().GetNameByIndex(Index);
        }

        friend constexpr bool operator==(TActorActivityType a, TActorActivityType b) = default;

        template <typename EEnum>
        friend bool operator==(TActorActivityType a, EEnum b) requires (std::is_enum_v<EEnum>) {
            return a == FromEnum(b);
        }

    private:
        explicit constexpr TActorActivityType(ui32 index)
            : Index(index)
        {}

    private:
        ui32 Index;
    };

    /**
     * A type erased actor task running within an actor
     */
    class TActorTask : public TIntrusiveListItem<TActorTask> {
    protected:
        ~TActorTask() = default;

    public:
        /**
         * Requests task to cancel
         */
        virtual void Cancel() noexcept = 0;

        /**
         * Requests task to stop and destroy itself immediately
         */
        virtual void Destroy() noexcept = 0;
    };

    /**
     * A type erased event awaiter, used for dynamic event dispatch
     */
    class TActorEventAwaiter : public TIntrusiveListItem<TActorEventAwaiter> {
    public:
        template<class TDerived>
        class TImpl;

        inline bool Handle(TAutoPtr<IEventHandle>& ev) {
            return (*HandleFn)(this, ev);
        }

    private:
        // All subclasses must use TImpl
        TActorEventAwaiter() = default;
        ~TActorEventAwaiter() = default;

    protected:
        // A single function pointer is cheaper than a vtable, may be changed at runtime and allows multiple instances in a class
        bool (*HandleFn)(TActorEventAwaiter*, TAutoPtr<IEventHandle>&);
    };

    /**
     * Base class for TActorEventAwaiter implementation classes
     */
    template<class TDerived>
    class TActorEventAwaiter::TImpl : public TActorEventAwaiter {
    public:
        TImpl() noexcept {
            this->HandleFn = +[](TActorEventAwaiter* self, TAutoPtr<IEventHandle>& ev) -> bool {
                return static_cast<TDerived&>(static_cast<TImpl&>(*self)).DoHandle(ev);
            };
        }

        explicit TImpl(bool (*handleFn)(TActorEventAwaiter*, TAutoPtr<IEventHandle>&)) noexcept {
            this->HandleFn = handleFn;
        }
    };

    /**
     * A type erased runnable item for local execution
     */
    class TActorRunnableItem : public TIntrusiveListItem<TActorRunnableItem> {
    public:
        template<class TDerived>
        class TImpl;

        inline void Run(IActor* actor) noexcept {
            (*RunFn)(this, actor);
        }

    private:
        // All subclasses must use TImpl
        TActorRunnableItem() = default;
        ~TActorRunnableItem() = default;

    protected:
        // A single function pointer is cheaper than a vtable, may be changed at runtime and allows multiple instances in a class
        void (*RunFn)(TActorRunnableItem*, IActor*) noexcept;
    };

    /**
     * Base class for TActorRunnableItem implementation classes
     */
    template<class TDerived>
    class TActorRunnableItem::TImpl : public TActorRunnableItem {
    public:
        TImpl() noexcept {
            this->RunFn = +[](TActorRunnableItem* self, IActor* actor) noexcept {
                static_cast<TDerived&>(static_cast<TImpl&>(*self)).DoRun(actor);
            };
        }

        explicit TImpl(void (*runFn)(TActorRunnableItem*, IActor*) noexcept) noexcept {
            this->RunFn = runFn;
        }
    };

    /**
     * A per-thread actor runnable queue available in an event handler
     */
    class TActorRunnableQueue {
    public:
        TActorRunnableQueue(IActor* actor) noexcept;
        ~TActorRunnableQueue();

        /**
         * Schedules a runnable item for execution
         */
        static void Schedule(TActorRunnableItem* runnable) noexcept;

        /**
         * Removes a runnable item from the queue
         */
        static void Cancel(TActorRunnableItem* runnable) noexcept;

        /**
         * Execute currently scheduled items
         */
        void Execute() noexcept;

    private:
        IActor* Actor_;
        TActorRunnableQueue* Prev_;
        TIntrusiveList<TActorRunnableItem> Queue_;
    };

    namespace NDetail {
        class TActorAsyncHandlerPromise;
        template<class TEvent>
        class TActorSpecificEventAwaiter;
    }

    class IActor
        : protected IActorOps
    {
    private:
        TActorIdentity SelfActorId;
        i64 ElapsedTicks;
        friend void DoActorInit(TActorSystem*, IActor*, const TActorId&, const TActorId&);
        friend class TDecorator;

    private:
        // actor aliases
        absl::flat_hash_set<ui64> Aliases;
        friend class TMailbox;

    private: // stuck actor monitoring
        TMonotonic LastReceiveTimestamp;
        size_t StuckIndex = Max<size_t>();
        friend class TExecutorPoolBaseMailboxed;
        friend class TExecutorThread;

    public:
        using TReceiveFunc = void (IActor::*)(TAutoPtr<IEventHandle>& ev);

    private:
        TReceiveFunc StateFunc_;

    private:
        friend class NDetail::TActorAsyncHandlerPromise;
        template<class TEvent>
        friend class NDetail::TActorSpecificEventAwaiter;

        TIntrusiveList<TActorTask> ActorTasks;
        absl::flat_hash_map<ui64, TIntrusiveList<TActorEventAwaiter>> EventAwaiters;
        bool PassedAway = false;

        void FinishPassAway();
        void DestroyActorTasks();
        bool RegisterActorTask(TActorTask* task);
        void UnregisterActorTask(TActorTask* task);
        void RegisterEventAwaiter(ui64 cookie, TActorEventAwaiter* awaiter);
        void UnregisterEventAwaiter(ui64 cookie, TActorEventAwaiter* awaiter);
        bool HandleResumeRunnable(TAutoPtr<IEventHandle>& ev);
        bool HandleRegisteredEvent(TAutoPtr<IEventHandle>& ev);

    public:
        using TEventFlags = IEventHandle::TEventFlags;
        /// @sa services.proto NKikimrServices::TActivity::EType
        using EActorActivity = EInternalActorType;
        using EActivityType = EActorActivity;

    private:
        TActorActivityType ActivityType;

    protected:
        ui64 HandledEvents;

        IActor(TReceiveFunc stateFunc, TActorActivityType activityType = {})
            : SelfActorId(TActorId())
            , ElapsedTicks(0)
            , StateFunc_(stateFunc)
            , ActivityType(activityType)
            , HandledEvents(0)
        {
        }

        template <typename EEnum>
        IActor(TReceiveFunc stateFunc, EEnum activityType) requires (std::is_enum_v<EEnum>)
            : IActor(stateFunc, TActorActivityType::FromEnum(activityType))
        {}

        IActor(TReceiveFunc stateFunc, TStringBuf activityName)
            : IActor(stateFunc, TActorActivityType::FromName(activityName))
        {}

        template <typename T>
        void Become(T stateFunc) {
            StateFunc_ = static_cast<TReceiveFunc>(stateFunc);
        }

        template <typename T, typename... TArgs>
        void Become(T stateFunc, TArgs&&... args) {
            StateFunc_ = static_cast<TReceiveFunc>(stateFunc);
            Schedule(std::forward<TArgs>(args)...);
        }

        template <typename T, typename... TArgs>
        void Become(T stateFunc, const TActorContext& ctx, TArgs&&... args) {
            StateFunc_ = static_cast<TReceiveFunc>(stateFunc);
            ctx.Schedule(std::forward<TArgs>(args)...);
        }

        TReceiveFunc CurrentStateFunc() const {
            return StateFunc_;
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
        void SetActivityType(TActorActivityType activityType);

        template <typename EEnum>
        void SetActivityType(EEnum activityType) requires (std::is_enum_v<EEnum>) {
            SetActivityType(TActorActivityType::FromEnum(activityType));
        }

        void SetActivityType(TStringBuf activityName) {
            SetActivityType(TActorActivityType::FromName(activityName));
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
        TActorActivityType GetActivityType() const {
            return ActivityType;
        }
        ui64 GetHandledEvents() const {
            return HandledEvents;
        }
        TActorIdentity SelfId() const {
            return SelfActorId;
        }

        void Receive(TAutoPtr<IEventHandle>& ev);

        TActorContext ActorContext() const {
            return TActivationContext::ActorContextFor(SelfId());
        }

    private:
        bool OnUnhandledExceptionSafe(const std::exception_ptr& exc);

    protected:
        void SetEnoughCpu(bool isEnough);

        void Describe(IOutputStream&) const override;
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

        TActorId RegisterAlias() noexcept final;
        void UnregisterAlias(const TActorId& actorId) noexcept final;

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

    inline TStringBuf GetActivityTypeName(TActorActivityType activityType) {
        return activityType.GetName();
    }

    // For compatibility with existing code
    using IActorCallback = IActor;

    template <typename TDerived>
    class TActor: public IActor {
    private:
        using TDerivedReceiveFunc = void (TDerived::*)(TAutoPtr<IEventHandle>& ev);

        static TActorActivityType GetDefaultActivityTypeImpl() {
            if constexpr (requires { TDerived::ActorName; }) {
                return TActorActivityType::FromStaticName<TDerived::ActorName>();
            } else if constexpr (requires { TDerived::ActorActivityType; }) {
                return TActorActivityType::FromEnum(TDerived::ActorActivityType());
            } else {
                return TActorActivityType::FromTypeName<TDerived>();
            }
        }

        static TActorActivityType GetDefaultActivityType() {
            static const TActorActivityType result = GetDefaultActivityTypeImpl();
            return result;
        }

    protected:
        // static constexpr char ActorName[] = "UNNAMED";

        TActor(TDerivedReceiveFunc func)
            : IActorCallback(static_cast<TReceiveFunc>(func), GetDefaultActivityType()) {
        }

        template <typename T>
        TActor(TDerivedReceiveFunc func, T&& activityType)
            : IActorCallback(static_cast<TReceiveFunc>(func), std::forward<T>(activityType))
        {}

    public:
        typedef TDerived TThis;

        // UnsafeBecome methods don't verify the bindings of the stateFunc to the TDerived
        template <typename T>
        void UnsafeBecome(T stateFunc) {
            this->IActor::Become(stateFunc);
        }

        template <typename T, typename... TArgs>
        void UnsafeBecome(T stateFunc, const TActorContext& ctx, TArgs&&... args) {
            this->IActor::Become(stateFunc, ctx, std::forward<TArgs>(args)...);
        }

        template <typename T, typename... TArgs>
        void UnsafeBecome(T stateFunc, TArgs&&... args) {
            this->IActor::Become(stateFunc, std::forward<TArgs>(args)...);
        }

        template <typename T>
        void Become(T stateFunc) {
            // TODO(kruall): have to uncomment asserts after end of sync contrib/ydb
            // static_assert(std::is_convertible_v<T, TDerivedReceiveFunc>);
            this->IActor::Become(stateFunc);
        }

        template <typename T, typename... TArgs>
        void Become(T stateFunc, const TActorContext& ctx, TArgs&&... args) {
            // static_assert(std::is_convertible_v<T, TDerivedReceiveFunc>);
            this->IActor::Become(stateFunc, ctx, std::forward<TArgs>(args)...);
        }

        template <typename T, typename... TArgs>
        void Become(T stateFunc, TArgs&&... args) {
            // static_assert(std::is_convertible_v<T, TDerivedReceiveFunc>);
            this->IActor::Become(stateFunc, std::forward<TArgs>(args)...);
        }
    };


#define STFUNC_SIG TAutoPtr<::NActors::IEventHandle>& ev
#define STATEFN_SIG TAutoPtr<::NActors::IEventHandle>& ev
#define STFUNC(funcName) void funcName(TAutoPtr<::NActors::IEventHandle>& ev)
#define STATEFN(funcName) void funcName(TAutoPtr<::NActors::IEventHandle>& ev)

#define STFUNC_STRICT_UNHANDLED_MSG_HANDLER Y_DEBUG_ABORT_UNLESS(false, "%s: unexpected message type %s 0x%08" PRIx32, __func__, ev->GetTypeName().c_str(), etype);

#define STFUNC_BODY(HANDLERS, UNHANDLED_MSG_HANDLER)                    \
    switch ([[maybe_unused]] const ui32 etype = ev->GetTypeRewrite()) { \
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

        bool BeforeSending(std::unique_ptr<IEventHandle>& ev) {
            TAutoPtr<IEventHandle> evPtr = ev.release();
            bool result = BeforeSending(evPtr);
            ev.reset(evPtr.Release());
            return result;
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

template<> struct std::hash<NActors::TActorIdentity> : THash<NActors::TActorIdentity> {};
