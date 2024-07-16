#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/executor_thread.h>
#include <ydb/library/actors/core/mailbox.h>
#include <ydb/library/actors/core/monotonic_provider.h>
#include <ydb/library/actors/util/should_continue.h>
#include <ydb/library/actors/interconnect/poller_tcp.h>
#include <ydb/library/actors/interconnect/mock/ic_mock.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/datetime/base.h>
#include <util/folder/tempdir.h>
#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>
#include <util/generic/queue.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/system/defaults.h>
#include <util/system/mutex.h>
#include <util/system/condvar.h>
#include <util/system/thread.h>
#include <util/system/sanitizers.h>
#include <util/system/valgrind.h>
#include <utility>

#include <functional>

const TDuration DEFAULT_DISPATCH_TIMEOUT = NSan::PlainOrUnderSanitizer(
        NValgrind::PlainOrUnderValgrind(TDuration::Seconds(60), TDuration::Seconds(120)),
        TDuration::Seconds(120)
);


namespace NActors {
    struct THeSingleSystemEnv { };

    struct TTestActorSetupCmd { // like TActorSetupCmd, but not owning the Actor
        TTestActorSetupCmd(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId)
            : MailboxType(mailboxType)
            , PoolId(poolId)
            , Actor(actor)
        {
        }

       TTestActorSetupCmd(TActorSetupCmd cmd)
            : MailboxType(cmd.MailboxType)
            , PoolId(cmd.PoolId)
            , Actor(cmd.Actor.release())
        {
        }

        TMailboxType::EType MailboxType;
        ui32 PoolId;
        IActor* Actor;
    };

    struct TEventMailboxId {
        TEventMailboxId()
            : NodeId(0)
            , Hint(0)
        {
        }

        TEventMailboxId(ui32 nodeId, ui32 hint)
            : NodeId(nodeId)
            , Hint(hint)
        {
        }

        bool operator<(const TEventMailboxId& other) const {
            return (NodeId < other.NodeId) || (NodeId == other.NodeId) && (Hint < other.Hint);
        }

        bool operator==(const TEventMailboxId& other) const {
            return (NodeId == other.NodeId) && (Hint == other.Hint);
        }

        struct THash {
            ui64 operator()(const TEventMailboxId& mboxId) const noexcept {
                return mboxId.NodeId * 31ULL + mboxId.Hint;
            }
        };

        ui32 NodeId;
        ui32 Hint;
    };

    struct TDispatchOptions {
        struct TFinalEventCondition {
            std::function<bool(IEventHandle& ev)> EventCheck;
            ui32 RequiredCount;

            TFinalEventCondition(ui32 eventType, ui32 requiredCount = 1)
                : EventCheck([eventType](IEventHandle& ev) -> bool { return ev.GetTypeRewrite() == eventType; })
                , RequiredCount(requiredCount)
            {
            }

            TFinalEventCondition(std::function<bool(IEventHandle& ev)> eventCheck, ui32 requiredCount = 1)
                : EventCheck(eventCheck)
                , RequiredCount(requiredCount)
            {
            }
        };

        TVector<TFinalEventCondition> FinalEvents;
        TVector<TEventMailboxId> NonEmptyMailboxes;
        TVector<TEventMailboxId> OnlyMailboxes;
        std::function<bool()> CustomFinalCondition;
        bool Quiet = false;
    };

    struct TScheduledEventQueueItem {
        TInstant Deadline;
        TAutoPtr<IEventHandle> Event;
        TAutoPtr<TSchedulerCookieHolder> Cookie;
        ui64 UniqueId;

        TScheduledEventQueueItem(TInstant deadline, TAutoPtr<IEventHandle> event, ISchedulerCookie* cookie)
            : Deadline(deadline)
            , Event(event)
            , Cookie(new TSchedulerCookieHolder(cookie))
            , UniqueId(++NextUniqueId)
        {}

        bool operator<(const TScheduledEventQueueItem& other) const {
            if (Deadline < other.Deadline)
                return true;

            if (Deadline > other.Deadline)
                return false;

            return UniqueId < other.UniqueId;
        }

        static ui64 NextUniqueId;
    };

    typedef TDeque<TAutoPtr<IEventHandle>> TEventsList;
    typedef TSet<TScheduledEventQueueItem> TScheduledEventsList;

    class TEventMailBox : public TThrRefBase {
    public:
        TEventMailBox()
            : InactiveUntil(TInstant::MicroSeconds(0))
#ifdef DEBUG_ORDER_EVENTS
            , ExpectedReceive(0)
            , NextToSend(0)
#endif
        {
        }

        void Send(TAutoPtr<IEventHandle> ev);
        bool IsEmpty() const;
        TAutoPtr<IEventHandle> Pop();
        void Capture(TEventsList& evList);
        void PushFront(TAutoPtr<IEventHandle>& ev);
        void PushFront(TEventsList& evList);
        void CaptureScheduled(TScheduledEventsList& evList);
        void PushScheduled(TScheduledEventsList& evList);
        bool IsActive(const TInstant& currentTime) const;
        void Freeze(const TInstant& deadline);
        TInstant GetInactiveUntil() const;
        void Schedule(const TScheduledEventQueueItem& item);
        bool IsScheduledEmpty() const;
        TInstant GetFirstScheduleDeadline() const;
        ui64 GetSentEventCount() const;

    private:
        TScheduledEventsList Scheduled;
        TInstant InactiveUntil;
        TEventsList Sent;
#ifdef DEBUG_ORDER_EVENTS
        TMap<IEventHandle*, ui64> TrackSent;
        ui64 ExpectedReceive;
        ui64 NextToSend;
#endif
    };

    typedef THashMap<TEventMailboxId, TIntrusivePtr<TEventMailBox>, TEventMailboxId::THash> TEventMailBoxList;

    class TEmptyEventQueueException : public yexception {
    public:
        TEmptyEventQueueException() {
            Append("Event queue is still empty.");
        }
    };

    class TSchedulingLimitReachedException : public yexception {
    public:
        TSchedulingLimitReachedException(ui64 limit) {
            TStringStream str;
            str << "TestActorRuntime Processed over " << limit << " events.";
            Append(str.Str());
        }
    };

    class TTestActorRuntimeBase: public TNonCopyable {
    public:
        class TEdgeActor;
        class TSchedulerThreadStub;
        class TExecutorPoolStub;
        class TTimeProvider;
        class TMonotonicTimeProvider;

        enum class EEventAction {
            PROCESS,
            DROP,
            RESCHEDULE
        };

        typedef std::function<EEventAction(TAutoPtr<IEventHandle>& event)> TEventObserver;
        typedef std::function<void(TTestActorRuntimeBase& runtime, TScheduledEventsList& scheduledEvents, TEventsList& queue)> TScheduledEventsSelector;
        typedef std::function<bool(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)> TEventFilter;
        typedef std::function<bool(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event, TDuration delay, TInstant& deadline)> TScheduledEventFilter;
        typedef std::function<void(TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId)> TRegistrationObserver;


        TTestActorRuntimeBase(THeSingleSystemEnv);
        TTestActorRuntimeBase(ui32 nodeCount, ui32 dataCenterCount, bool UseRealThreads);
        TTestActorRuntimeBase(ui32 nodeCount, ui32 dataCenterCount);
        TTestActorRuntimeBase(ui32 nodeCount = 1, bool useRealThreads = false);
        virtual ~TTestActorRuntimeBase();
        bool IsRealThreads() const;
        static EEventAction DefaultObserverFunc(TAutoPtr<IEventHandle>& event);
        static void DroppingScheduledEventsSelector(TTestActorRuntimeBase& runtime, TScheduledEventsList& scheduledEvents, TEventsList& queue);
        static void CollapsedTimeScheduledEventsSelector(TTestActorRuntimeBase& runtime, TScheduledEventsList& scheduledEvents, TEventsList& queue);
        static bool DefaultFilterFunc(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event);
        static bool NopFilterFunc(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event, TDuration delay, TInstant& deadline);
        static void DefaultRegistrationObserver(TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId);
        TEventObserver SetObserverFunc(TEventObserver observerFunc);  // deprecated, use AddObserver
        TScheduledEventsSelector SetScheduledEventsSelectorFunc(TScheduledEventsSelector scheduledEventsSelectorFunc);
        TEventFilter SetEventFilter(TEventFilter filterFunc);
        TScheduledEventFilter SetScheduledEventFilter(TScheduledEventFilter filterFunc);
        TRegistrationObserver SetRegistrationObserverFunc(TRegistrationObserver observerFunc);
        static bool IsVerbose();
        static void SetVerbose(bool verbose);
        TDuration SetDispatchTimeout(TDuration timeout);
        void SetDispatchedEventsLimit(ui64 limit) {
            DispatchedEventsLimit = limit;
        }
        TDuration SetReschedulingDelay(TDuration delay);
        void SetLogBackend(const TAutoPtr<TLogBackend> logBackend);
        void SetLogBackendFactory(std::function<TAutoPtr<TLogBackend>()> logBackendFactory);
        void SetLogPriority(NActors::NLog::EComponent component, NActors::NLog::EPriority priority);
        TIntrusivePtr<ITimeProvider> GetTimeProvider();
        TIntrusivePtr<IMonotonicTimeProvider> GetMonotonicTimeProvider();
        TInstant GetCurrentTime() const;
        TMonotonic GetCurrentMonotonicTime() const;
        /**
         * When `rewind` is true allows time to go backwards. This is unsafe,
         * since both wallclock and monotonic times are currently linked and
         * both go backwards, but it may be necessary for testing wallclock
         * time oddities.
         */
        void UpdateCurrentTime(TInstant newTime, bool rewind = false);
        void AdvanceCurrentTime(TDuration duration);
        void AddLocalService(const TActorId& actorId, TActorSetupCmd cmd, ui32 nodeIndex = 0);
        virtual void Initialize();
        ui32 GetNodeId(ui32 index = 0) const;
        ui32 GetNodeCount() const;
        ui64 AllocateLocalId();
        ui32 InterconnectPoolId() const;
        TString GetTempDir();
        TActorId Register(IActor* actor, ui32 nodeIndex = 0, ui32 poolId = 0,
            TMailboxType::EType mailboxType = TMailboxType::Simple, ui64 revolvingCounter = 0,
            const TActorId& parentid = TActorId());
        TActorId Register(IActor *actor, ui32 nodeIndex, ui32 poolId, TMailboxHeader *mailbox, ui32 hint,
            const TActorId& parentid = TActorId());
        TActorId RegisterService(const TActorId& serviceId, const TActorId& actorId, ui32 nodeIndex = 0);
        TActorId AllocateEdgeActor(ui32 nodeIndex = 0);
        TEventsList CaptureEvents();
        TEventsList CaptureMailboxEvents(ui32 hint, ui32 nodeId);
        TScheduledEventsList CaptureScheduledEvents();
        void PushFront(TAutoPtr<IEventHandle>& ev);
        void PushEventsFront(TEventsList& events);
        void PushMailboxEventsFront(ui32 hint, ui32 nodeId, TEventsList& events);
        // doesn't dispatch events for edge actors
        bool DispatchEvents(const TDispatchOptions& options = TDispatchOptions());
        bool DispatchEvents(const TDispatchOptions& options, TDuration simTimeout);
        bool DispatchEvents(const TDispatchOptions& options, TInstant simDeadline);
        void Send(const TActorId& recipient, const TActorId& sender, TAutoPtr<IEventBase> ev, ui32 senderNodeIndex = 0, bool viaActorSystem = false);
        void Send(TAutoPtr<IEventHandle> ev, ui32 senderNodeIndex = 0, bool viaActorSystem = false);
        void SendAsync(TAutoPtr<IEventHandle> ev, ui32 senderNodeIndex = 0);
        void Schedule(TAutoPtr<IEventHandle> ev, const TDuration& duration, ui32 nodeIndex = 0);
        void ClearCounters();
        ui64 GetCounter(ui32 evType) const;
        TActorId GetLocalServiceId(const TActorId& serviceId, ui32 nodeIndex = 0);
        void WaitForEdgeEvents(TEventFilter filter, const TSet<TActorId>& edgeFilter = {}, TDuration simTimeout = TDuration::Max());
        TActorId GetInterconnectProxy(ui32 nodeIndexFrom, ui32 nodeIndexTo);
        void BlockOutputForActor(const TActorId& actorId);
        IActor* FindActor(const TActorId& actorId, ui32 nodeIndex = Max<ui32>()) const;
        TStringBuf FindActorName(const TActorId& actorId, ui32 nodeIndex = Max<ui32>()) const;
        void EnableScheduleForActor(const TActorId& actorId, bool allow = true);
        bool IsScheduleForActorEnabled(const TActorId& actorId) const;
        TIntrusivePtr<NMonitoring::TDynamicCounters> GetDynamicCounters(ui32 nodeIndex = 0);
        void SetupMonitoring(ui16 monitoringPortOffset = 0, bool monitoringTypeAsync = false);

        using TEventObserverCollection = std::list<std::function<void(TAutoPtr<IEventHandle>& event)>>;
        class TEventObserverHolder {
        public:
            TEventObserverHolder()
                : List(nullptr)
            {
            }

            TEventObserverHolder(TEventObserverCollection* list, TEventObserverCollection::iterator&& iter)
                : List(list)
                , Iter(iter)
            {
            }

            TEventObserverHolder(TEventObserverHolder&& other)
                : List(nullptr)
            {
                *this = std::move(other);
            }

            TEventObserverHolder& operator=(TEventObserverHolder&& other) noexcept {
                if (this != &other)
                {
                    Remove();

                    List = std::move(other.List);
                    Iter = std::move(other.Iter);

                    other.List = nullptr;
                }
                return *this;
            }

            ~TEventObserverHolder()
            {
                Remove();
            }

            void Remove()
            {
                if (List == nullptr || Iter == List->end()) {
                    return;
                }

                List->erase(Iter);
                Iter = List->end();

                List = nullptr;
            }
        private:
            TEventObserverCollection* List;
            TEventObserverCollection::iterator Iter;
        };
        using TEventObserverHolderPair = std::pair<TEventObserverHolder,TEventObserverHolder>;

        // An example of using AddObserver in unit tests
        /*
            auto observerHolder = runtime.AddObserver<TEvDataShard::TEvRead>([&](TEvDataShard::TEvRead::TPtr& event) {
                // Do something with the event inside the callback
                Cout << "An event is observed " << event->Get()->Record.ShortDebugString() << Endl;

                // Optionally reset the event, all subsequent handlers of this event will not be called
                event.Reset();
            });

            // Do something inside the main code of the unit test

            // Optionally remove the observer, otherwise it will be destroyed in its destructor
            observerHolder.Remove();
        */

        template <typename TEvType>
        TEventObserverHolder AddObserver(std::function<void(typename TEvType::TPtr&)> observerFunc)
        {
            auto baseFunc = [observerFunc](TAutoPtr<IEventHandle>& event) {
                if (event && event->GetTypeRewrite() == TEvType::EventType)
                    observerFunc(*(reinterpret_cast<typename TEvType::TPtr*>(&event)));
            };

            auto iter = ObserverFuncs.insert(ObserverFuncs.end(), baseFunc);
            return TEventObserverHolder(&ObserverFuncs, std::move(iter));
        }

        TEventObserverHolder AddObserver(std::function<void(TAutoPtr<IEventHandle>&)> observerFunc)
        {
            auto iter = ObserverFuncs.insert(ObserverFuncs.end(), observerFunc);
            return TEventObserverHolder(&ObserverFuncs, std::move(iter));
        }

        template<typename T>
        void AppendToLogSettings(NLog::EComponent minVal, NLog::EComponent maxVal, T func) {
            Y_ABORT_UNLESS(!IsInitialized);

            for (const auto& pair : Nodes) {
                pair.second->LogSettings->Append(minVal, maxVal, func);
            }
        }

        TIntrusivePtr<NLog::TSettings> GetLogSettings(ui32 nodeIdx)
        {
            return Nodes[FirstNodeId + nodeIdx]->LogSettings;
        }

        TActorSystem* SingleSys() const;
        TActorSystem* GetAnyNodeActorSystem();
        TActorSystem* GetActorSystem(ui32 nodeId);
        template <typename TEvent>
        TEvent* GrabEdgeEventIf(TAutoPtr<IEventHandle>& handle, std::function<bool(const TEvent&)> predicate, TDuration simTimeout = TDuration::Max()) {
            handle.Destroy();
            const ui32 eventType = TEvent::EventType;
            WaitForEdgeEvents([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() != eventType)
                    return false;

                TEvent* typedEvent = event->Get<TEvent>();
                if (predicate(*typedEvent)) {
                    handle = event;
                    return true;
                }

                return false;
            }, {}, simTimeout);

            if (simTimeout == TDuration::Max())
                Y_ABORT_UNLESS(handle);

            if (handle) {
                return handle->Get<TEvent>();
            } else {
                return nullptr;
            }
        }

        template<class TEvent>
        typename TEvent::TPtr GrabEdgeEventIf(
                const TSet<TActorId>& edgeFilter,
                const std::function<bool(const typename TEvent::TPtr&)>& predicate,
                TDuration simTimeout = TDuration::Max())
        {
            typename TEvent::TPtr handle;
            const ui32 eventType = TEvent::EventType;
            WaitForEdgeEvents([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() != eventType)
                    return false;

                typename TEvent::TPtr* typedEvent = reinterpret_cast<typename TEvent::TPtr*>(&event);
                if (predicate(*typedEvent)) {
                    handle = *typedEvent;
                    return true;
                }

                return false;
            }, edgeFilter, simTimeout);

            if (simTimeout == TDuration::Max())
                Y_ABORT_UNLESS(handle);

            return handle;
        }

        template<class TEvent>
        typename TEvent::TPtr GrabEdgeEventIf(
                const TActorId& edgeActor,
                const std::function<bool(const typename TEvent::TPtr&)>& predicate,
                TDuration simTimeout = TDuration::Max())
        {
            TSet<TActorId> edgeFilter{edgeActor};
            return GrabEdgeEventIf<TEvent>(edgeFilter, predicate, simTimeout);
        }

        template <typename TEvent>
        TEvent* GrabEdgeEvent(TAutoPtr<IEventHandle>& handle, TDuration simTimeout = TDuration::Max()) {
            std::function<bool(const TEvent&)> truth = [](const TEvent&) { return true; };
            return GrabEdgeEventIf(handle, truth, simTimeout);
        }

        template <typename TEvent>
        THolder<TEvent> GrabEdgeEvent(TDuration simTimeout = TDuration::Max()) {
            TAutoPtr<IEventHandle> handle;
            std::function<bool(const TEvent&)> truth = [](const TEvent&) { return true; };
            GrabEdgeEventIf(handle, truth, simTimeout);
            if (handle) {
                return THolder<TEvent>(handle->Release<TEvent>());
            }
            return {};
        }

        template<class TEvent>
        typename TEvent::TPtr GrabEdgeEvent(const TSet<TActorId>& edgeFilter, TDuration simTimeout = TDuration::Max()) {
            return GrabEdgeEventIf<TEvent>(edgeFilter, [](const typename TEvent::TPtr&) { return true; }, simTimeout);
        }

        template<class TEvent>
        typename TEvent::TPtr GrabEdgeEvent(const TActorId& edgeActor, TDuration simTimeout = TDuration::Max()) {
            TSet<TActorId> edgeFilter{edgeActor};
            return GrabEdgeEvent<TEvent>(edgeFilter, simTimeout);
        }

        // replace with std::variant<>
        template <typename... TEvents>
        std::tuple<TEvents*...> GrabEdgeEvents(TAutoPtr<IEventHandle>& handle, TDuration simTimeout = TDuration::Max()) {
            handle.Destroy();
            auto eventTypes = { TEvents::EventType... };
            WaitForEdgeEvents([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                if (std::find(std::begin(eventTypes), std::end(eventTypes), event->GetTypeRewrite()) == std::end(eventTypes))
                    return false;
                handle = event;
                return true;
            }, {}, simTimeout);
            if (simTimeout == TDuration::Max())
                Y_ABORT_UNLESS(handle);
            if (handle) {
                return std::make_tuple(handle->Type == TEvents::EventType
                                       ? handle->Get<TEvents>()
                                       : static_cast<TEvents*>(nullptr)...);
            }
            return {};
        }

        template <typename TEvent>
        TEvent* GrabEdgeEventRethrow(TAutoPtr<IEventHandle>& handle, TDuration simTimeout = TDuration::Max()) {
            try {
                return GrabEdgeEvent<TEvent>(handle, simTimeout);
            } catch (...) {
                ythrow TWithBackTrace<yexception>() << "Exception occured while waiting for " << TypeName<TEvent>() << ": " << CurrentExceptionMessage();
            }
        }

        template<class TEvent>
        typename TEvent::TPtr GrabEdgeEventRethrow(const TSet<TActorId>& edgeFilter, TDuration simTimeout = TDuration::Max()) {
            try {
                return GrabEdgeEvent<TEvent>(edgeFilter, simTimeout);
            } catch (...) {
                ythrow TWithBackTrace<yexception>() << "Exception occured while waiting for " << TypeName<TEvent>() << ": " << CurrentExceptionMessage();
            }
        }

        template<class TEvent>
        typename TEvent::TPtr GrabEdgeEventRethrow(const TActorId& edgeActor, TDuration simTimeout = TDuration::Max()) {
            try {
                return GrabEdgeEvent<TEvent>(edgeActor, simTimeout);
            } catch (...) {
                ythrow TWithBackTrace<yexception>() << "Exception occured while waiting for " << TypeName<TEvent>() << ": " << CurrentExceptionMessage();
            }
        }

        template <typename... TEvents>
        static TString TypeNames() {
            static TString names[] = { TypeName<TEvents>()... };
            TString result;
            for (const TString& s : names) {
                if (result.empty()) {
                    result += '<';
                } else {
                    result += ',';
                }
                result += s;
            }
            if (!result.empty()) {
                result += '>';
            }
            return result;
        }

        template <typename... TEvents>
        std::tuple<TEvents*...> GrabEdgeEventsRethrow(TAutoPtr<IEventHandle>& handle, TDuration simTimeout = TDuration::Max()) {
            try {
                return GrabEdgeEvents<TEvents...>(handle, simTimeout);
            } catch (...) {
                ythrow TWithBackTrace<yexception>() << "Exception occured while waiting for " << TypeNames<TEvents...>() << ": " << CurrentExceptionMessage();
            }
        }

        void ResetScheduledCount() {
            ScheduledCount = 0;
        }

        void SetScheduledLimit(ui64 limit) {
            ScheduledLimit = limit;
        }

        void SetDispatcherRandomSeed(TInstant time, ui64 iteration);
        TString GetActorName(const TActorId& actorId) const;

        const TVector<ui64>& GetTxAllocatorTabletIds() const { return TxAllocatorTabletIds; }
        void SetTxAllocatorTabletIds(const TVector<ui64>& ids) { TxAllocatorTabletIds = ids; }

        void SetUseRealInterconnect() {
            UseRealInterconnect = true;
        }

        void SetICCommonSetupper(std::function<void(ui32, TIntrusivePtr<TInterconnectProxyCommon>)>&& icCommonSetupper) {
            ICCommonSetupper = std::move(icCommonSetupper);
        }

    protected:
        struct TNodeDataBase;
        TNodeDataBase* GetRawNode(ui32 node) const {
            return Nodes.at(FirstNodeId + node).Get();
        }

        static IExecutorPool* CreateExecutorPoolStub(TTestActorRuntimeBase* runtime, ui32 nodeIndex, TNodeDataBase* node, ui32 poolId);
        virtual TIntrusivePtr<NMonitoring::TDynamicCounters> GetCountersForComponent(TIntrusivePtr<NMonitoring::TDynamicCounters> counters, const char* component) {
            Y_UNUSED(counters);
            Y_UNUSED(component);

            // do nothing, just return the existing counters
            return counters;
        }

        THolder<TActorSystemSetup> MakeActorSystemSetup(ui32 nodeIndex, TNodeDataBase* node);
        THolder<TActorSystem> MakeActorSystem(ui32 nodeIndex, TNodeDataBase* node);
        virtual void InitActorSystemSetup(TActorSystemSetup& setup, TNodeDataBase* node) {
            Y_UNUSED(setup, node);
        }

   private:
        IActor* FindActor(const TActorId& actorId, TNodeDataBase* node) const;
        void SendInternal(TAutoPtr<IEventHandle> ev, ui32 nodeIndex, bool viaActorSystem);
        TEventMailBox& GetMailbox(ui32 nodeId, ui32 hint);
        void ClearMailbox(ui32 nodeId, ui32 hint);
        void HandleNonEmptyMailboxesForEachContext(TEventMailboxId mboxId);
        void UpdateFinalEventsStatsForEachContext(IEventHandle& ev);
        bool DispatchEventsInternal(const TDispatchOptions& options, TInstant simDeadline);

    private:
        ui64 ScheduledCount;
        ui64 ScheduledLimit;
        THolder<TTempDir> TmpDir;
        const TThread::TId MainThreadId;

    protected:
        bool UseRealInterconnect = false;
        TInterconnectMock InterconnectMock;
        bool IsInitialized = false;
        bool SingleSysEnv = false;
        const TString ClusterUUID;
        const ui32 FirstNodeId;
        const ui32 NodeCount;
        const ui32 DataCenterCount;
        const bool UseRealThreads;
        std::function<void(ui32, TIntrusivePtr<TInterconnectProxyCommon>)> ICCommonSetupper;

        ui64 LocalId;
        TMutex Mutex;
        TCondVar MailboxesHasEvents;
        TEventMailBoxList Mailboxes;
        TMap<ui32, ui64> EvCounters;
        ui64 DispatchCyclesCount;
        ui64 DispatchedEventsCount;
        ui64 DispatchedEventsLimit = 2'500'000;
        TActorId CurrentRecipient;
        ui64 DispatcherRandomSeed;
        TIntrusivePtr<IRandomProvider> DispatcherRandomProvider;
        TAutoPtr<TLogBackend> LogBackend;
        std::function<TAutoPtr<TLogBackend>()> LogBackendFactory;
        bool NeedMonitoring;
        ui16 MonitoringPortOffset = 0;
        bool MonitoringTypeAsync = false;

        TIntrusivePtr<IRandomProvider> RandomProvider;
        TIntrusivePtr<ITimeProvider> TimeProvider;
        TIntrusivePtr<IMonotonicTimeProvider> MonotonicTimeProvider;

    protected:
        struct TNodeDataBase: public TThrRefBase {
            TNodeDataBase();
            void Stop();
            virtual ~TNodeDataBase();
            virtual ui64 GetLoggerPoolId() const {
                return 0;
            }

            template <typename T = void>
            T* GetAppData() {
                return static_cast<T*>(AppData0.get());
            }

            template <typename T = void>
            const T* GetAppData() const {
                return static_cast<T*>(AppData0.get());
            }

            TIntrusivePtr<NMonitoring::TDynamicCounters> DynamicCounters;
            TIntrusivePtr<NActors::NLog::TSettings> LogSettings;
            TIntrusivePtr<NInterconnect::TPollerThreads> Poller;
            volatile ui64* ActorSystemTimestamp;
            volatile ui64* ActorSystemMonotonic;
            TVector<std::pair<TActorId, TTestActorSetupCmd>> LocalServices;
            TMap<TActorId, IActor*> LocalServicesActors;
            TMap<IActor*, TActorId> ActorToActorId;
            THolder<TMailboxTable> MailboxTable;
            std::shared_ptr<void> AppData0;
            THolder<TActorSystem> ActorSystem;
            THolder<IExecutorPool> SchedulerPool;
            TVector<IExecutorPool*> ExecutorPools;
            THolder<TExecutorThread> ExecutorThread;
            std::unique_ptr<IHarmonizer> Harmonizer;
        };

        struct INodeFactory {
            virtual ~INodeFactory() = default;
            virtual TIntrusivePtr<TNodeDataBase> CreateNode() = 0;
        };

        struct TDefaultNodeFactory final: INodeFactory {
            virtual TIntrusivePtr<TNodeDataBase> CreateNode() override {
                return new TNodeDataBase();
            }
        };

        INodeFactory& GetNodeFactory() {
            return *NodeFactory;
        }

        virtual TNodeDataBase* GetNodeById(size_t idx) {
            return Nodes[idx].Get();
        }

        void InitNodes();
        void CleanupNodes();
        virtual void InitNodeImpl(TNodeDataBase*, size_t);

        static bool AllowSendFrom(TNodeDataBase* node, TAutoPtr<IEventHandle>& ev);

    protected:
        THolder<INodeFactory> NodeFactory{new TDefaultNodeFactory};

    private:
        void InitNode(TNodeDataBase* node, size_t idx);

        struct TDispatchContext {
            const TDispatchOptions* Options;
            TDispatchContext* PrevContext;

            TMap<const TDispatchOptions::TFinalEventCondition*, ui32> FinalEventFrequency;
            TSet<TEventMailboxId> FoundNonEmptyMailboxes;
            bool FinalEventFound = false;
        };

        TProgramShouldContinue ShouldContinue;
        TMap<ui32, TIntrusivePtr<TNodeDataBase>> Nodes;
        ui64 CurrentTimestamp;
        TSet<TActorId> EdgeActors;
        THashMap<TEventMailboxId, TActorId, TEventMailboxId::THash> EdgeActorByMailbox;
        TDuration DispatchTimeout;
        TDuration ReschedulingDelay;
        TEventObserver ObserverFunc;
        TEventObserverCollection ObserverFuncs;
        TScheduledEventsSelector ScheduledEventsSelectorFunc;
        TEventFilter EventFilterFunc;
        TScheduledEventFilter ScheduledEventFilterFunc;
        TRegistrationObserver RegistrationObserver;
        TSet<TActorId> BlockedOutput;
        TSet<TActorId> ScheduleWhiteList;
        THashMap<TActorId, TActorId> ScheduleWhiteListParent;
        THashMap<TActorId, TString> ActorNames;
        TDispatchContext* CurrentDispatchContext;
        TVector<ui64> TxAllocatorTabletIds;

        static ui32 NextNodeId;
    };

    template <typename TEvent>
    TEvent* FindEvent(TEventsList& events) {
        for (auto& event : events) {
            if (event && event->GetTypeRewrite() == TEvent::EventType) {
                return event->CastAsLocal<TEvent>();
            }
        }

        return nullptr;
    }

    template <typename TEvent>
    TEvent* FindEvent(TEventsList& events, const std::function<bool(const TEvent&)>& predicate) {
        for (auto& event : events) {
            if (event && event->GetTypeRewrite() == TEvent::EventType && predicate(*event->CastAsLocal<TEvent>())) {
                return event->CastAsLocal<TEvent>();
            }
        }

        return nullptr;
    }

    template <typename TEvent>
    TEvent* GrabEvent(TEventsList& events, TAutoPtr<IEventHandle>& ev) {
        ev.Destroy();
        for (auto& event : events) {
            if (event && event->GetTypeRewrite() == TEvent::EventType) {
                ev = event;
                return ev->CastAsLocal<TEvent>();
            }
        }

        return nullptr;
    }

    template <typename TEvent>
    TEvent* GrabEvent(TEventsList& events, TAutoPtr<IEventHandle>& ev,
        const std::function<bool(const typename TEvent::TPtr&)>& predicate) {
        ev.Destroy();
        for (auto& event : events) {
            if (event && event->GetTypeRewrite() == TEvent::EventType) {
                if (predicate(reinterpret_cast<const typename TEvent::TPtr&>(event))) {
                    ev = event;
                    return ev->CastAsLocal<TEvent>();
                }
            }
        }

        return nullptr;
    }

    class IStrandingDecoratorFactory {
    public:
        virtual ~IStrandingDecoratorFactory() {}
        virtual IActor* Wrap(const TActorId& delegatee, bool isSync, const TVector<TActorId>& additionalActors) = 0;
    };

    struct IReplyChecker {
        virtual ~IReplyChecker() {}
        virtual void OnRequest(IEventHandle *request) = 0;
        virtual bool IsWaitingForMoreResponses(IEventHandle *response) = 0;
    };

    struct TNoneReplyChecker : IReplyChecker {
        void OnRequest(IEventHandle*) override {
        }

        bool IsWaitingForMoreResponses(IEventHandle*) override {
            return false;
        }
    };

    using TReplyCheckerCreator = std::function<THolder<IReplyChecker>(void)>;

    inline THolder<IReplyChecker> CreateNoneReplyChecker() {
        return MakeHolder<TNoneReplyChecker>();
    }

    TAutoPtr<IStrandingDecoratorFactory> CreateStrandingDecoratorFactory(TTestActorRuntimeBase* runtime,
            TReplyCheckerCreator createReplyChecker = CreateNoneReplyChecker);
    extern ui64 DefaultRandomSeed;
}
