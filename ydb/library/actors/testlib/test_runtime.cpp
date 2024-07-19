#include "test_runtime.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/callstack.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_io.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/util/datetime.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <library/cpp/random_provider/random_provider.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_proxy.h>
#include <ydb/library/actors/interconnect/interconnect_proxy_wrapper.h>

#include <util/generic/maybe.h>
#include <util/generic/bt_exception.h>
#include <util/random/mersenne.h>
#include <util/string/printf.h>
#include <typeinfo>

bool VERBOSE = false;
const bool PRINT_EVENT_BODY = false;

namespace {

    TString MakeClusterId() {
        pid_t pid = getpid();
        TStringBuilder uuid;
        uuid << "Cluster for process with id: " << pid;
        return uuid;
    }
}

namespace NActors {
    ui64 TScheduledEventQueueItem::NextUniqueId = 0;

    void PrintEvent(TAutoPtr<IEventHandle>& ev, const TTestActorRuntimeBase* runtime) {
        Cerr << "mailbox: " << ev->GetRecipientRewrite().Hint() << ", type: " << Sprintf("%08x", ev->GetTypeRewrite())
            << ", from " << ev->Sender.LocalId();
        TString name = runtime->GetActorName(ev->Sender);
        if (!name.empty())
            Cerr << " \"" << name << "\"";
        Cerr << ", to " << ev->GetRecipientRewrite().LocalId();
        name = runtime->GetActorName(ev->GetRecipientRewrite());
        if (!name.empty())
            Cerr << " \"" << name << "\"";
        Cerr << ", ";
        if (ev->HasEvent())
            Cerr << " : " << (PRINT_EVENT_BODY ? ev->ToString() : ev->GetTypeName());
        else if (ev->HasBuffer())
            Cerr << " : BUFFER";
        else
            Cerr << " : EMPTY";

        Cerr << "\n";
    }

    TTestActorRuntimeBase::TNodeDataBase::TNodeDataBase() {
        ActorSystemTimestamp = nullptr;
        ActorSystemMonotonic = nullptr;
    }

    void TTestActorRuntimeBase::TNodeDataBase::Stop() {
        if (Poller)
            Poller->Stop();

        if (MailboxTable) {
            for (ui32 round = 0; !MailboxTable->Cleanup(); ++round)
                Y_ABORT_UNLESS(round < 10, "cyclic event/actor spawn while trying to shutdown actorsystem stub");
        }

        if (ActorSystem)
            ActorSystem->Stop();

        ActorSystem.Destroy();
        Poller.Reset();
    }

    TTestActorRuntimeBase::TNodeDataBase::~TNodeDataBase() {
        Stop();
    }


    class TTestActorRuntimeBase::TEdgeActor : public TActor<TEdgeActor> {
    public:
        static constexpr EActivityType ActorActivityType() {
            return EActivityType::TEST_ACTOR_RUNTIME;
        }

        TEdgeActor(TTestActorRuntimeBase* runtime)
            : TActor(&TEdgeActor::StateFunc)
            , Runtime(runtime)
        {
        }

        STFUNC(StateFunc) {
            TGuard<TMutex> guard(Runtime->Mutex);
            bool verbose = (Runtime->CurrentDispatchContext ? !Runtime->CurrentDispatchContext->Options->Quiet : true) && VERBOSE;
            if (Runtime->BlockedOutput.find(ev->Sender) != Runtime->BlockedOutput.end()) {
                verbose = false;
            }

            if (verbose) {
                Cerr << "Got event at " << TInstant::MicroSeconds(Runtime->CurrentTimestamp) << ", ";
                PrintEvent(ev, Runtime);
            }

            if (!Runtime->EventFilterFunc(*Runtime, ev)) {
                ui32 nodeId = ev->GetRecipientRewrite().NodeId();
                Y_ABORT_UNLESS(nodeId != 0);
                ui32 mailboxHint = ev->GetRecipientRewrite().Hint();
                Runtime->GetMailbox(nodeId, mailboxHint).Send(ev);
                Runtime->MailboxesHasEvents.Signal();
                if (verbose)
                    Cerr << "Event was added to sent queue\n";
            }
            else {
                if (verbose)
                    Cerr << "Event was dropped\n";
            }
        }

    private:
        TTestActorRuntimeBase* Runtime;
    };

    void TEventMailBox::Send(TAutoPtr<IEventHandle> ev) {
        IEventHandle* ptr = ev.Get();
        Y_ABORT_UNLESS(ptr);
#ifdef DEBUG_ORDER_EVENTS
        ui64 counter = NextToSend++;
        TrackSent[ptr] = counter;
#endif
        Sent.push_back(ev);
    }

    TAutoPtr<IEventHandle> TEventMailBox::Pop() {
        TAutoPtr<IEventHandle> result = Sent.front();
        Sent.pop_front();
#ifdef DEBUG_ORDER_EVENTS
        auto it = TrackSent.find(result.Get());
        if (it != TrackSent.end()) {
            Y_ABORT_UNLESS(ExpectedReceive == it->second);
            TrackSent.erase(result.Get());
            ++ExpectedReceive;
        }
#endif
        return result;
    }

    bool TEventMailBox::IsEmpty() const {
        return Sent.empty();
    }

    void TEventMailBox::Capture(TEventsList& evList) {
        evList.insert(evList.end(), Sent.begin(), Sent.end());
        Sent.clear();
    }

    void TEventMailBox::PushFront(TAutoPtr<IEventHandle>& ev) {
        Sent.push_front(ev);
    }

    void TEventMailBox::PushFront(TEventsList& evList) {
        for (auto rit = evList.rbegin(); rit != evList.rend(); ++rit) {
            if (*rit) {
                Sent.push_front(*rit);
            }
        }
    }

    void TEventMailBox::CaptureScheduled(TScheduledEventsList& evList) {
        for (auto it = Scheduled.begin(); it != Scheduled.end(); ++it) {
            evList.insert(*it);
        }

        Scheduled.clear();
    }

    void TEventMailBox::PushScheduled(TScheduledEventsList& evList) {
        for (auto it = evList.begin(); it != evList.end(); ++it) {
            if (it->Event) {
                Scheduled.insert(*it);
            }
        }

        evList.clear();
    }

    bool TEventMailBox::IsActive(const TInstant& currentTime) const {
        return currentTime >= InactiveUntil;
    }

    void TEventMailBox::Freeze(const TInstant& deadline) {
        if (deadline > InactiveUntil)
            InactiveUntil = deadline;
    }

    TInstant TEventMailBox::GetInactiveUntil() const {
        return InactiveUntil;
    }

    void TEventMailBox::Schedule(const TScheduledEventQueueItem& item) {
        Scheduled.insert(item);
    }

    bool TEventMailBox::IsScheduledEmpty() const {
        return Scheduled.empty();
    }

    TInstant TEventMailBox::GetFirstScheduleDeadline() const {
        return Scheduled.begin()->Deadline;
    }

    ui64 TEventMailBox::GetSentEventCount() const {
        return Sent.size();
    }

    class TTestActorRuntimeBase::TTimeProvider : public ITimeProvider {
    public:
        TTimeProvider(TTestActorRuntimeBase& runtime)
            : Runtime(runtime)
        {
        }

        TInstant Now() override {
            return Runtime.GetCurrentTime();
        }

    private:
        TTestActorRuntimeBase& Runtime;
    };

    class TTestActorRuntimeBase::TMonotonicTimeProvider : public IMonotonicTimeProvider {
    public:
        TMonotonicTimeProvider(TTestActorRuntimeBase& runtime)
            : Runtime(runtime)
        { }

        TMonotonic Now() override {
            return Runtime.GetCurrentMonotonicTime();
        }

    private:
        TTestActorRuntimeBase& Runtime;
    };

    class TTestActorRuntimeBase::TSchedulerThreadStub : public ISchedulerThread {
    public:
        TSchedulerThreadStub(TTestActorRuntimeBase* runtime, TTestActorRuntimeBase::TNodeDataBase* node)
            : Runtime(runtime)
            , Node(node)
        {
            Y_UNUSED(Runtime);
        }

        void Prepare(TActorSystem *actorSystem, volatile ui64 *currentTimestamp, volatile ui64 *currentMonotonic) override {
            Y_UNUSED(actorSystem);
            Node->ActorSystemTimestamp = currentTimestamp;
            Node->ActorSystemMonotonic = currentMonotonic;
        }

        void PrepareSchedules(NSchedulerQueue::TReader **readers, ui32 scheduleReadersCount) override {
            Y_UNUSED(readers);
            Y_UNUSED(scheduleReadersCount);
        }

        void Start() override {
        }

        void PrepareStop() override {
        }

        void Stop() override {
        }

    private:
        TTestActorRuntimeBase* Runtime;
        TTestActorRuntimeBase::TNodeDataBase* Node;
    };

    class TTestActorRuntimeBase::TExecutorPoolStub : public IExecutorPool {
    public:
        TExecutorPoolStub(TTestActorRuntimeBase* runtime, ui32 nodeIndex, TTestActorRuntimeBase::TNodeDataBase* node, ui32 poolId)
            : IExecutorPool(poolId)
            , Runtime(runtime)
            , NodeIndex(nodeIndex)
            , Node(node)
        {
        }

        TTestActorRuntimeBase* GetRuntime() {
            return Runtime;
        }

        // for threads
        ui32 GetReadyActivation(TWorkerContext& wctx, ui64 revolvingCounter) override {
            Y_UNUSED(wctx);
            Y_UNUSED(revolvingCounter);
            Y_ABORT();
        }

        void ReclaimMailbox(TMailboxType::EType mailboxType, ui32 hint, TWorkerId workerId, ui64 revolvingCounter) override {
            Y_UNUSED(workerId);
            Node->MailboxTable->ReclaimMailbox(mailboxType, hint, revolvingCounter);
        }

        TMailboxHeader *ResolveMailbox(ui32 hint) override {
            return Node->MailboxTable->Get(hint);
        }

        void Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie *cookie, TWorkerId workerId) override {
            DoSchedule(deadline, ev, cookie, workerId);
        }

        void Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie *cookie, TWorkerId workerId) override {
            DoSchedule(TInstant::FromValue(deadline.GetValue()), ev, cookie, workerId);
        }

        void Schedule(TDuration delay, TAutoPtr<IEventHandle> ev, ISchedulerCookie *cookie, TWorkerId workerId) override {
            TInstant deadline = Runtime->GetTimeProvider()->Now() + delay;
            DoSchedule(deadline, ev, cookie, workerId);
        }

        void DoSchedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie *cookie, TWorkerId workerId) {
            Y_UNUSED(workerId);

            TGuard<TMutex> guard(Runtime->Mutex);
            bool verbose = (Runtime->CurrentDispatchContext ? !Runtime->CurrentDispatchContext->Options->Quiet : true) && VERBOSE;
            if (Runtime->BlockedOutput.find(ev->Sender) != Runtime->BlockedOutput.end()) {
                verbose = false;
            }

            if (verbose) {
                Cerr << "Got scheduled event at " << TInstant::MicroSeconds(Runtime->CurrentTimestamp) << ", ";
                PrintEvent(ev, Runtime);
            }

            auto now = Runtime->GetTimeProvider()->Now();
            if (deadline < now) {
                deadline = now; // avoid going backwards in time
            }
            TDuration delay = (deadline - now);

            if (Runtime->SingleSysEnv || !Runtime->ScheduledEventFilterFunc(*Runtime, ev, delay, deadline)) {
                ui32 mailboxHint = ev->GetRecipientRewrite().Hint();
                Runtime->GetMailbox(Runtime->FirstNodeId + NodeIndex, mailboxHint).Schedule(TScheduledEventQueueItem(deadline, ev, cookie));
                Runtime->MailboxesHasEvents.Signal();
                if (verbose)
                    Cerr << "Event was added to scheduled queue\n";
            } else {
                if (cookie) {
                    cookie->Detach();
                }
                if (verbose) {
                    Cerr << "Scheduled event for " << ev->GetRecipientRewrite().ToString() << " was dropped\n";
                }
            }
        }

        // for actorsystem
        bool SpecificSend(TAutoPtr<IEventHandle>& ev) override {
            return Send(ev);
        }

        bool Send(TAutoPtr<IEventHandle>& ev) override {
            TGuard<TMutex> guard(Runtime->Mutex);
            bool verbose = (Runtime->CurrentDispatchContext ? !Runtime->CurrentDispatchContext->Options->Quiet : true) && VERBOSE;
            if (Runtime->BlockedOutput.find(ev->Sender) != Runtime->BlockedOutput.end()) {
                verbose = false;
            }

            if (verbose) {
                Cerr << "Got event at " << TInstant::MicroSeconds(Runtime->CurrentTimestamp) << ", ";
                PrintEvent(ev, Runtime);
            }

            if (!Runtime->EventFilterFunc(*Runtime, ev)) {
                ui32 nodeId = ev->GetRecipientRewrite().NodeId();
                Y_ABORT_UNLESS(nodeId != 0);
                TNodeDataBase* node = Runtime->Nodes[nodeId].Get();

                if (!AllowSendFrom(node, ev)) {
                    return true;
                }

                ui32 mailboxHint = ev->GetRecipientRewrite().Hint();
                if (ev->GetTypeRewrite() == ui32(NActors::NLog::EEv::Log)) {
                    const NActors::TActorId loggerActorId = NActors::TActorId(nodeId, "logger");
                    TActorId logger = node->ActorSystem->LookupLocalService(loggerActorId);
                    if (ev->GetRecipientRewrite() == logger) {
                        TMailboxHeader* mailbox = node->MailboxTable->Get(mailboxHint);
                        IActor* recipientActor = mailbox->FindActor(ev->GetRecipientRewrite().LocalId());
                        if (recipientActor) {
                            TActorContext ctx(*mailbox, *node->ExecutorThread, GetCycleCountFast(), ev->GetRecipientRewrite());
                            TActivationContext *prevTlsActivationContext = TlsActivationContext;
                            TlsActivationContext = &ctx;
                            recipientActor->Receive(ev);
                            TlsActivationContext = prevTlsActivationContext;
                            // we expect the logger to never die in tests
                        }
                    }
                } else {
                    Runtime->GetMailbox(nodeId, mailboxHint).Send(ev);
                    Runtime->MailboxesHasEvents.Signal();
                }
                if (verbose)
                    Cerr << "Event was added to sent queue\n";
            } else {
                if (verbose)
                    Cerr << "Event was dropped\n";
            }
            return true;
        }

        void ScheduleActivation(ui32 activation) override {
            Y_UNUSED(activation);
        }

        void SpecificScheduleActivation(ui32 activation) override {
            Y_UNUSED(activation);
        }

        void ScheduleActivationEx(ui32 activation, ui64 revolvingCounter) override {
            Y_UNUSED(activation);
            Y_UNUSED(revolvingCounter);
        }

        TActorId Register(IActor *actor, TMailboxType::EType mailboxType, ui64 revolvingCounter,
            const TActorId& parentId) override {
            return Runtime->Register(actor, NodeIndex, PoolId, mailboxType, revolvingCounter, parentId);
        }

        TActorId Register(IActor *actor, TMailboxHeader *mailbox, ui32 hint, const TActorId& parentId) override {
            return Runtime->Register(actor, NodeIndex, PoolId, mailbox, hint, parentId);
        }

        // lifecycle stuff
        void Prepare(TActorSystem *actorSystem, NSchedulerQueue::TReader **scheduleReaders, ui32 *scheduleSz) override {
            Y_UNUSED(actorSystem);
            Y_UNUSED(scheduleReaders);
            Y_UNUSED(scheduleSz);
        }

        void Start() override {
        }

        void PrepareStop() override {
        }

        void Shutdown() override {
        }

        bool Cleanup() override {
            return true;
        }

        // generic
        TAffinity* Affinity() const override {
            Y_ABORT();
        }

    private:
        TTestActorRuntimeBase* const Runtime;
        const ui32 NodeIndex;
        TTestActorRuntimeBase::TNodeDataBase* const Node;
    };

    IExecutorPool* TTestActorRuntimeBase::CreateExecutorPoolStub(TTestActorRuntimeBase* runtime, ui32 nodeIndex, TTestActorRuntimeBase::TNodeDataBase* node, ui32 poolId) {
        return new TExecutorPoolStub{runtime, nodeIndex, node, poolId};
    }


    ui32 TTestActorRuntimeBase::NextNodeId = 1;

    TTestActorRuntimeBase::TTestActorRuntimeBase(THeSingleSystemEnv)
        : TTestActorRuntimeBase(1, 1, false)
    {
        SingleSysEnv = true;
    }

    TTestActorRuntimeBase::TTestActorRuntimeBase(ui32 nodeCount, ui32 dataCenterCount, bool useRealThreads)
        : ScheduledCount(0)
        , ScheduledLimit(100000)
        , MainThreadId(TThread::CurrentThreadId())
        , ClusterUUID(MakeClusterId())
        , FirstNodeId(NextNodeId)
        , NodeCount(nodeCount)
        , DataCenterCount(dataCenterCount)
        , UseRealThreads(useRealThreads)
        , LocalId(0)
        , DispatchCyclesCount(0)
        , DispatchedEventsCount(0)
        , NeedMonitoring(false)
        , RandomProvider(CreateDeterministicRandomProvider(DefaultRandomSeed))
        , TimeProvider(new TTimeProvider(*this))
        , MonotonicTimeProvider(new TMonotonicTimeProvider(*this))
        , ShouldContinue()
        , CurrentTimestamp(0)
        , DispatchTimeout(DEFAULT_DISPATCH_TIMEOUT)
        , ReschedulingDelay(TDuration::MicroSeconds(0))
        , ObserverFunc(&TTestActorRuntimeBase::DefaultObserverFunc)
        , ScheduledEventsSelectorFunc(&CollapsedTimeScheduledEventsSelector)
        , EventFilterFunc(&TTestActorRuntimeBase::DefaultFilterFunc)
        , ScheduledEventFilterFunc(&TTestActorRuntimeBase::NopFilterFunc)
        , RegistrationObserver(&TTestActorRuntimeBase::DefaultRegistrationObserver)
        , CurrentDispatchContext(nullptr)
    {
        SetDispatcherRandomSeed(TInstant::Now(), 0);
        EnableActorCallstack();
    }

    void TTestActorRuntimeBase::InitNode(TNodeDataBase* node, size_t nodeIndex) {
        const NActors::TActorId loggerActorId = NActors::TActorId(FirstNodeId + nodeIndex, "logger");
        node->LogSettings = new NActors::NLog::TSettings(loggerActorId, NActorsServices::LOGGER,
            NActors::NLog::PRI_WARN,  NActors::NLog::PRI_WARN, 0);
        node->LogSettings->SetAllowDrop(false);
        node->LogSettings->SetThrottleDelay(TDuration::Zero());
        node->DynamicCounters = new NMonitoring::TDynamicCounters;

        InitNodeImpl(node, nodeIndex);
    }

    void TTestActorRuntimeBase::InitNodeImpl(TNodeDataBase* node, size_t nodeIndex) {
        node->LogSettings->Append(
            NActorsServices::EServiceCommon_MIN,
            NActorsServices::EServiceCommon_MAX,
            NActorsServices::EServiceCommon_Name
        );

        if (!UseRealThreads) {
            node->SchedulerPool.Reset(CreateExecutorPoolStub(this, nodeIndex, node, 0));
            node->MailboxTable.Reset(new TMailboxTable());
            node->ActorSystem = MakeActorSystem(nodeIndex, node);
            node->ExecutorThread.Reset(new TExecutorThread(0, 0, node->ActorSystem.Get(), node->SchedulerPool.Get(), node->MailboxTable.Get(), "TestExecutor"));
        } else {
            node->ActorSystem = MakeActorSystem(nodeIndex, node);
        }

        node->ActorSystem->Start();
    }

    bool TTestActorRuntimeBase::AllowSendFrom(TNodeDataBase* node, TAutoPtr<IEventHandle>& ev) {
        ui64 senderLocalId = ev->Sender.LocalId();
        ui64 senderMailboxHint = ev->Sender.Hint();
        TMailboxHeader* senderMailbox = node->MailboxTable->Get(senderMailboxHint);
        if (senderMailbox) {
            IActor* senderActor = senderMailbox->FindActor(senderLocalId);
            TTestDecorator *decorator = dynamic_cast<TTestDecorator*>(senderActor);
            return !decorator || decorator->BeforeSending(ev);
        }
        return true;
    }

    TTestActorRuntimeBase::TTestActorRuntimeBase(ui32 nodeCount, ui32 dataCenterCount)
        : TTestActorRuntimeBase(nodeCount, dataCenterCount, false) {
    }

    TTestActorRuntimeBase::TTestActorRuntimeBase(ui32 nodeCount, bool useRealThreads)
        : TTestActorRuntimeBase(nodeCount, nodeCount, useRealThreads) {
    }

    TTestActorRuntimeBase::~TTestActorRuntimeBase() {
        CleanupNodes();
        Cerr.Flush();
        Cerr.Flush();
        Clog.Flush();

        DisableActorCallstack();
    }

    void TTestActorRuntimeBase::CleanupNodes() {
        Nodes.clear();
    }

    bool TTestActorRuntimeBase::IsRealThreads() const {
        return UseRealThreads;
    }

    TTestActorRuntimeBase::EEventAction TTestActorRuntimeBase::DefaultObserverFunc(TAutoPtr<IEventHandle>& event) {
        Y_UNUSED(event);
        return EEventAction::PROCESS;
    }

    void TTestActorRuntimeBase::DroppingScheduledEventsSelector(TTestActorRuntimeBase& runtime, TScheduledEventsList& scheduledEvents, TEventsList& queue) {
        Y_UNUSED(runtime);
        Y_UNUSED(queue);
        scheduledEvents.clear();
    }

    bool TTestActorRuntimeBase::DefaultFilterFunc(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
        Y_UNUSED(runtime);
        Y_UNUSED(event);
        return false;
    }

    bool TTestActorRuntimeBase::NopFilterFunc(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event, TDuration delay, TInstant& deadline) {
        Y_UNUSED(runtime);
        Y_UNUSED(delay);
        Y_UNUSED(event);
        Y_UNUSED(deadline);
        return true;
    }


    void TTestActorRuntimeBase::DefaultRegistrationObserver(TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
        if (runtime.ScheduleWhiteList.find(parentId) != runtime.ScheduleWhiteList.end()) {
            runtime.ScheduleWhiteList.insert(actorId);
            runtime.ScheduleWhiteListParent[actorId] = parentId;
        }
    }

    class TScheduledTreeItem {
    public:
        TString Name;
        ui64 Count;
        TVector<TScheduledTreeItem> Children;

        TScheduledTreeItem(const TString& name)
            : Name(name)
            , Count(0)
        {}

        TScheduledTreeItem* GetItem(const TString& name) {
            TScheduledTreeItem* item = nullptr;
            for (TScheduledTreeItem& i : Children) {
                if (i.Name == name) {
                    item = &i;
                    break;
                }
            }
            if (item != nullptr)
                return item;
            Children.emplace_back(name);
            return &Children.back();
        }

        void RecursiveSort() {
            Sort(Children, [](const TScheduledTreeItem& a, const TScheduledTreeItem& b) -> bool { return a.Count > b.Count; });
            for (TScheduledTreeItem& item : Children) {
                item.RecursiveSort();
            }
        }

        void Print(IOutputStream& stream, const TString& prefix) {
            for (auto it = Children.begin(); it != Children.end(); ++it) {
                bool lastChild = (std::next(it) == Children.end());
                TString connectionPrefix = lastChild ? "└─ " : "├─ ";
                TString subChildPrefix = lastChild ? "   " : "│  ";
                stream << prefix << connectionPrefix << it->Name << " (" << it->Count << ")\n";
                it->Print(stream, prefix + subChildPrefix);
            }
        }

        void Print(IOutputStream& stream) {
            stream << Name << " (" << Count << ")\n";
            Print(stream, TString());
        }
    };

    void TTestActorRuntimeBase::CollapsedTimeScheduledEventsSelector(TTestActorRuntimeBase& runtime, TScheduledEventsList& scheduledEvents, TEventsList& queue) {
        if (scheduledEvents.empty())
            return;

        TInstant time = scheduledEvents.begin()->Deadline;
        while (!scheduledEvents.empty() && scheduledEvents.begin()->Deadline == time) {
//            static THashMap<std::pair<TActorId, TString>, ui64> eventTypes;
            auto& item = *scheduledEvents.begin();
            TString name = item.Event->GetTypeName();
//            eventTypes[std::make_pair(item.Event->Recipient, name)]++;
            runtime.ScheduledCount++;
            if (runtime.ScheduledCount > runtime.ScheduledLimit) {
//                TScheduledTreeItem root("Root");
//                TVector<TString> path;
//                for (const auto& pr : eventTypes) {
//                    path.clear();
//                    path.push_back(runtime.GetActorName(pr.first.first));
//                    for (auto it = runtime.ScheduleWhiteListParent.find(pr.first.first); it != runtime.ScheduleWhiteListParent.end(); it = runtime.ScheduleWhiteListParent.find(it->second)) {
//                        path.insert(path.begin(), runtime.GetActorName(it->second));
//                    }
//                    path.push_back("<" + pr.first.second + ">"); // event name;
//                    ui64 count = pr.second;
//                    TScheduledTreeItem* item = &root;
//                    item->Count += count;
//                    for (TString name : path) {
//                        item = item->GetItem(name);
//                        item->Count += count;
//                    }
//                }
//                root.RecursiveSort();
//                root.Print(Cerr);

                ythrow TSchedulingLimitReachedException(runtime.ScheduledLimit);
            }
            if (item.Cookie->Get()) {
                if (item.Cookie->Detach()) {
                    queue.push_back(item.Event);
                }
            } else {
                queue.push_back(item.Event);
            }

            scheduledEvents.erase(scheduledEvents.begin());
        }

        runtime.UpdateCurrentTime(time);
    }

    TTestActorRuntimeBase::TEventObserver TTestActorRuntimeBase::SetObserverFunc(TEventObserver observerFunc) {
        TGuard<TMutex> guard(Mutex);
        auto result = ObserverFunc;
        ObserverFunc = observerFunc;
        return result;
    }

    TTestActorRuntimeBase::TScheduledEventsSelector TTestActorRuntimeBase::SetScheduledEventsSelectorFunc(TScheduledEventsSelector scheduledEventsSelectorFunc) {
        TGuard<TMutex> guard(Mutex);
        auto result = ScheduledEventsSelectorFunc;
        ScheduledEventsSelectorFunc = scheduledEventsSelectorFunc;
        return result;
    }

    TTestActorRuntimeBase::TEventFilter TTestActorRuntimeBase::SetEventFilter(TEventFilter filterFunc) {
        TGuard<TMutex> guard(Mutex);
        auto result = EventFilterFunc;
        EventFilterFunc = filterFunc;
        return result;
    }

    TTestActorRuntimeBase::TScheduledEventFilter TTestActorRuntimeBase::SetScheduledEventFilter(TScheduledEventFilter filterFunc) {
        TGuard<TMutex> guard(Mutex);
        auto result = ScheduledEventFilterFunc;
        ScheduledEventFilterFunc = filterFunc;
        return result;
    }

    TTestActorRuntimeBase::TRegistrationObserver TTestActorRuntimeBase::SetRegistrationObserverFunc(TRegistrationObserver observerFunc) {
        TGuard<TMutex> guard(Mutex);
        auto result = RegistrationObserver;
        RegistrationObserver = observerFunc;
        return result;
    }

    bool TTestActorRuntimeBase::IsVerbose() {
        return VERBOSE;
    }

    void TTestActorRuntimeBase::SetVerbose(bool verbose) {
        VERBOSE = verbose;
    }

    void TTestActorRuntimeBase::AddLocalService(const TActorId& actorId, TActorSetupCmd cmd, ui32 nodeIndex) {
        Y_ABORT_UNLESS(!IsInitialized);
        Y_ABORT_UNLESS(nodeIndex < NodeCount);
        auto node = Nodes[nodeIndex + FirstNodeId];
        if (!node) {
            node = GetNodeFactory().CreateNode();
            Nodes[nodeIndex + FirstNodeId] = node;
        }

        node->LocalServicesActors[actorId] = cmd.Actor.get();
        node->LocalServices.push_back(std::make_pair(actorId, TTestActorSetupCmd(std::move(cmd))));
    }

    void TTestActorRuntimeBase::InitNodes() {
        NextNodeId += NodeCount;
        Y_ABORT_UNLESS(NodeCount > 0);

        for (ui32 nodeIndex = 0; nodeIndex < NodeCount; ++nodeIndex) {
            auto nodeIt = Nodes.emplace(FirstNodeId + nodeIndex, GetNodeFactory().CreateNode()).first;
            TNodeDataBase* node = nodeIt->second.Get();
            InitNode(node, nodeIndex);
        }

    }

    void TTestActorRuntimeBase::Initialize() {
        InitNodes();
        IsInitialized = true;
    }

    void SetupCrossDC() {

    }

    TDuration TTestActorRuntimeBase::SetDispatchTimeout(TDuration timeout) {
        TGuard<TMutex> guard(Mutex);
        TDuration oldTimeout = DispatchTimeout;
        DispatchTimeout = timeout;
        return oldTimeout;
    }

    TDuration TTestActorRuntimeBase::SetReschedulingDelay(TDuration delay) {
        TGuard<TMutex> guard(Mutex);
        TDuration oldDelay = ReschedulingDelay;
        ReschedulingDelay = delay;
        return oldDelay;
    }

    void TTestActorRuntimeBase::SetLogBackend(const TAutoPtr<TLogBackend> logBackend) {
        Y_ABORT_UNLESS(!IsInitialized);
        TGuard<TMutex> guard(Mutex);
        LogBackend = logBackend;
    }

    void TTestActorRuntimeBase::SetLogBackendFactory(std::function<TAutoPtr<TLogBackend>()> logBackendFactory) {
        Y_ABORT_UNLESS(!IsInitialized);
        TGuard<TMutex> guard(Mutex);
        LogBackendFactory = logBackendFactory;
    }

    void TTestActorRuntimeBase::SetLogPriority(NActors::NLog::EComponent component, NActors::NLog::EPriority priority) {
        TGuard<TMutex> guard(Mutex);
        for (ui32 nodeIndex = 0; nodeIndex < NodeCount; ++nodeIndex) {
            TNodeDataBase* node = Nodes[FirstNodeId + nodeIndex].Get();
            TString explanation;
            auto status = node->LogSettings->SetLevel(priority, component, explanation);
            if (status) {
                Y_ABORT("SetLogPriority failed: %s", explanation.c_str());
            }
        }
    }

    TInstant TTestActorRuntimeBase::GetCurrentTime() const {
        TGuard<TMutex> guard(Mutex);
        Y_ABORT_UNLESS(!UseRealThreads);
        return TInstant::MicroSeconds(CurrentTimestamp);
    }

    TMonotonic TTestActorRuntimeBase::GetCurrentMonotonicTime() const {
        TGuard<TMutex> guard(Mutex);
        Y_ABORT_UNLESS(!UseRealThreads);
        return TMonotonic::MicroSeconds(CurrentTimestamp);
    }

    void TTestActorRuntimeBase::UpdateCurrentTime(TInstant newTime, bool rewind) {
        static int counter = 0;
        ++counter;
        if (VERBOSE) {
            Cerr << "UpdateCurrentTime(" << counter << "," << newTime << ")\n";
        }
        TGuard<TMutex> guard(Mutex);
        Y_ABORT_UNLESS(!UseRealThreads);
        if (rewind || newTime.MicroSeconds() > CurrentTimestamp) {
            CurrentTimestamp = newTime.MicroSeconds();
            for (auto& kv : Nodes) {
                AtomicStore(kv.second->ActorSystemTimestamp, CurrentTimestamp);
                AtomicStore(kv.second->ActorSystemMonotonic, CurrentTimestamp);
            }
        }
    }

    void TTestActorRuntimeBase::AdvanceCurrentTime(TDuration duration) {
        UpdateCurrentTime(GetCurrentTime() + duration);
    }

    TIntrusivePtr<ITimeProvider> TTestActorRuntimeBase::GetTimeProvider() {
        Y_ABORT_UNLESS(!UseRealThreads);
        return TimeProvider;
    }

    TIntrusivePtr<IMonotonicTimeProvider> TTestActorRuntimeBase::GetMonotonicTimeProvider() {
        Y_ABORT_UNLESS(!UseRealThreads);
        return MonotonicTimeProvider;
    }

    ui32 TTestActorRuntimeBase::GetNodeId(ui32 index) const {
        Y_ABORT_UNLESS(index < NodeCount);
        return FirstNodeId + index;
    }

    ui32 TTestActorRuntimeBase::GetNodeCount() const {
        return NodeCount;
    }

    ui64 TTestActorRuntimeBase::AllocateLocalId() {
        TGuard<TMutex> guard(Mutex);
        ui64 nextId = ++LocalId;
        if (VERBOSE) {
            Cerr << "Allocated id: " << nextId << "\n";
        }

        return nextId;
    }

    ui32 TTestActorRuntimeBase::InterconnectPoolId() const {
        if (UseRealThreads && NSan::TSanIsOn()) {
            // Interconnect coroutines may move across threads
            // Use a special single-threaded pool to avoid that
            return 4;
        }
        return 0;
    }

    TString TTestActorRuntimeBase::GetTempDir() {
        if (!TmpDir)
            TmpDir.Reset(new TTempDir());
        return (*TmpDir)();
    }

    TActorId TTestActorRuntimeBase::Register(IActor* actor, ui32 nodeIndex, ui32 poolId, TMailboxType::EType mailboxType,
        ui64 revolvingCounter, const TActorId& parentId) {
        Y_ABORT_UNLESS(nodeIndex < NodeCount);
        TGuard<TMutex> guard(Mutex);
        TNodeDataBase* node = Nodes[FirstNodeId + nodeIndex].Get();
        if (UseRealThreads) {
            Y_ABORT_UNLESS(poolId < node->ExecutorPools.size());
            return node->ExecutorPools[poolId]->Register(actor, mailboxType, revolvingCounter, parentId);
        }

        // first step - find good enough mailbox
        ui32 hint = 0;
        TMailboxHeader *mailbox = nullptr;

        {
            ui32 hintBackoff = 0;

            while (hint == 0) {
                hint = node->MailboxTable->AllocateMailbox(mailboxType, ++revolvingCounter);
                mailbox = node->MailboxTable->Get(hint);

                if (!mailbox->LockFromFree()) {
                    node->MailboxTable->ReclaimMailbox(mailboxType, hintBackoff, ++revolvingCounter);
                    hintBackoff = hint;
                    hint = 0;
                }
            }

            node->MailboxTable->ReclaimMailbox(mailboxType, hintBackoff, ++revolvingCounter);
        }

        const ui64 localActorId = AllocateLocalId();
        if (VERBOSE) {
            Cerr << "Register actor " << TypeName(*actor) << " as " << localActorId << ", mailbox: " << hint << "\n";
        }

        // ok, got mailbox
        mailbox->AttachActor(localActorId, actor);

        // do init
        const TActorId actorId(FirstNodeId + nodeIndex, poolId, localActorId, hint);
        ActorNames[actorId] = TypeName(*actor);
        RegistrationObserver(*this, parentId ? parentId : CurrentRecipient, actorId);
        DoActorInit(node->ActorSystem.Get(), actor, actorId, parentId ? parentId : CurrentRecipient);

        switch (mailboxType) {
        case TMailboxType::Simple:
            UnlockFromExecution((TMailboxTable::TSimpleMailbox *)mailbox, node->ExecutorPools[0], false, hint, MaxWorkers, ++revolvingCounter);
            break;
        case TMailboxType::Revolving:
            UnlockFromExecution((TMailboxTable::TRevolvingMailbox *)mailbox, node->ExecutorPools[0], false, hint, MaxWorkers, ++revolvingCounter);
            break;
        case TMailboxType::HTSwap:
            UnlockFromExecution((TMailboxTable::THTSwapMailbox *)mailbox, node->ExecutorPools[0], false, hint, MaxWorkers, ++revolvingCounter);
            break;
        case TMailboxType::ReadAsFilled:
            UnlockFromExecution((TMailboxTable::TReadAsFilledMailbox *)mailbox, node->ExecutorPools[0], false, hint, MaxWorkers, ++revolvingCounter);
            break;
        case TMailboxType::TinyReadAsFilled:
            UnlockFromExecution((TMailboxTable::TTinyReadAsFilledMailbox *)mailbox, node->ExecutorPools[0], false, hint, MaxWorkers, ++revolvingCounter);
            break;
        default:
            Y_ABORT("Unsupported mailbox type");
        }

        return actorId;
    }

    TActorId TTestActorRuntimeBase::Register(IActor *actor, ui32 nodeIndex, ui32 poolId, TMailboxHeader *mailbox, ui32 hint,
        const TActorId& parentId) {
        Y_ABORT_UNLESS(nodeIndex < NodeCount);
        TGuard<TMutex> guard(Mutex);
        TNodeDataBase* node = Nodes[FirstNodeId + nodeIndex].Get();
        if (UseRealThreads) {
            Y_ABORT_UNLESS(poolId < node->ExecutorPools.size());
            return node->ExecutorPools[poolId]->Register(actor, mailbox, hint, parentId);
        }

        const ui64 localActorId = AllocateLocalId();
        if (VERBOSE) {
            Cerr << "Register actor " << TypeName(*actor) << " as " << localActorId << "\n";
        }

        mailbox->AttachActor(localActorId, actor);
        const TActorId actorId(FirstNodeId + nodeIndex, poolId, localActorId, hint);
        ActorNames[actorId] = TypeName(*actor);
        RegistrationObserver(*this, parentId ? parentId : CurrentRecipient, actorId);
        DoActorInit(node->ActorSystem.Get(), actor, actorId, parentId ? parentId : CurrentRecipient);

        return actorId;
    }

    TActorId TTestActorRuntimeBase::RegisterService(const TActorId& serviceId, const TActorId& actorId, ui32 nodeIndex) {
        TGuard<TMutex> guard(Mutex);
        Y_ABORT_UNLESS(nodeIndex < NodeCount);
        TNodeDataBase* node = Nodes[FirstNodeId + nodeIndex].Get();
        if (!UseRealThreads) {
            IActor* actor = FindActor(actorId, node);
            node->LocalServicesActors[serviceId] = actor;
            node->ActorToActorId[actor] = actorId;
        }

        return node->ActorSystem->RegisterLocalService(serviceId, actorId);
    }

    TActorId TTestActorRuntimeBase::AllocateEdgeActor(ui32 nodeIndex) {
        TGuard<TMutex> guard(Mutex);
        Y_ABORT_UNLESS(nodeIndex < NodeCount);
        TActorId edgeActor = Register(new TEdgeActor(this), nodeIndex);
        EdgeActors.insert(edgeActor);
        EdgeActorByMailbox[TEventMailboxId(edgeActor.NodeId(), edgeActor.Hint())] = edgeActor;
        return edgeActor;
    }

    TEventsList TTestActorRuntimeBase::CaptureEvents() {
        TGuard<TMutex> guard(Mutex);
        TEventsList result;
        for (auto& mbox : Mailboxes) {
            mbox.second->Capture(result);
        }

        return result;
    }

    TEventsList TTestActorRuntimeBase::CaptureMailboxEvents(ui32 hint, ui32 nodeId) {
        TGuard<TMutex> guard(Mutex);
        Y_ABORT_UNLESS(nodeId >= FirstNodeId && nodeId < FirstNodeId + NodeCount);
        TEventsList result;
        GetMailbox(nodeId, hint).Capture(result);
        return result;
    }

    void TTestActorRuntimeBase::PushFront(TAutoPtr<IEventHandle>& ev) {
        TGuard<TMutex> guard(Mutex);
        ui32 nodeId = ev->GetRecipientRewrite().NodeId();
        Y_ABORT_UNLESS(nodeId != 0);
        GetMailbox(nodeId, ev->GetRecipientRewrite().Hint()).PushFront(ev);
    }

    void TTestActorRuntimeBase::PushEventsFront(TEventsList& events) {
        TGuard<TMutex> guard(Mutex);
        for (auto rit = events.rbegin(); rit != events.rend(); ++rit) {
            if (*rit) {
                auto& ev = *rit;
                ui32 nodeId = ev->GetRecipientRewrite().NodeId();
                Y_ABORT_UNLESS(nodeId != 0);
                GetMailbox(nodeId, ev->GetRecipientRewrite().Hint()).PushFront(ev);
            }
        }

        events.clear();
    }

    void TTestActorRuntimeBase::PushMailboxEventsFront(ui32 hint, ui32 nodeId, TEventsList& events) {
        TGuard<TMutex> guard(Mutex);
        Y_ABORT_UNLESS(nodeId >= FirstNodeId && nodeId < FirstNodeId + NodeCount);
        TEventsList result;
        GetMailbox(nodeId, hint).PushFront(events);
        events.clear();
    }

    TScheduledEventsList TTestActorRuntimeBase::CaptureScheduledEvents() {
        TGuard<TMutex> guard(Mutex);
        TScheduledEventsList result;
        for (auto& mbox : Mailboxes) {
            mbox.second->CaptureScheduled(result);
        }

        return result;
    }

    bool TTestActorRuntimeBase::DispatchEvents(const TDispatchOptions& options) {
        return DispatchEvents(options, TInstant::Max());
    }

    bool TTestActorRuntimeBase::DispatchEvents(const TDispatchOptions& options, TDuration simTimeout) {
        return DispatchEvents(options, TInstant::MicroSeconds(CurrentTimestamp) + simTimeout);
    }

    bool TTestActorRuntimeBase::DispatchEvents(const TDispatchOptions& options, TInstant simDeadline) {
        TGuard<TMutex> guard(Mutex);
        return DispatchEventsInternal(options, simDeadline);
    }

    // Mutex must be locked by caller!
    bool TTestActorRuntimeBase::DispatchEventsInternal(const TDispatchOptions& options, TInstant simDeadline) {
        TDispatchContext localContext;
        localContext.Options = &options;
        localContext.PrevContext = nullptr;
        bool verbose = !options.Quiet && VERBOSE;

        struct TDispatchContextSetter {
            TDispatchContextSetter(TTestActorRuntimeBase& runtime, TDispatchContext& lastContext)
                : Runtime(runtime)
            {
                lastContext.PrevContext = Runtime.CurrentDispatchContext;
                Runtime.CurrentDispatchContext = &lastContext;
            }

            ~TDispatchContextSetter() {
                Runtime.CurrentDispatchContext = Runtime.CurrentDispatchContext->PrevContext;
            }

            TTestActorRuntimeBase& Runtime;
        } DispatchContextSetter(*this, localContext);

        TInstant dispatchTime = TInstant::MicroSeconds(0);
        TInstant deadline = dispatchTime + DispatchTimeout;
        const TDuration scheduledEventsInspectInterval = TDuration::MilliSeconds(10);
        TInstant inspectScheduledEventsAt = dispatchTime + scheduledEventsInspectInterval;
        if (verbose) {
            Cerr << "Start dispatch at " << TInstant::MicroSeconds(CurrentTimestamp) << ", deadline is " << deadline << "\n";
        }

        struct TTempEdgeEventsCaptor {
            TTempEdgeEventsCaptor(TTestActorRuntimeBase& runtime)
                : Runtime(runtime)
                , HasEvents(false)
            {
                for (auto edgeActor : Runtime.EdgeActors) {
                    TEventsList events;
                    Runtime.GetMailbox(edgeActor.NodeId(), edgeActor.Hint()).Capture(events);
                    auto mboxId = TEventMailboxId(edgeActor.NodeId(), edgeActor.Hint());
                    auto storeIt = Store.find(mboxId);
                    Y_ABORT_UNLESS(storeIt == Store.end());
                    storeIt = Store.insert(std::make_pair(mboxId, new TEventMailBox)).first;
                    storeIt->second->PushFront(events);
                    if (!events.empty())
                        HasEvents = true;
                }
            }

            ~TTempEdgeEventsCaptor() {
                for (auto edgeActor : Runtime.EdgeActors) {
                    auto mboxId = TEventMailboxId(edgeActor.NodeId(), edgeActor.Hint());
                    auto storeIt = Store.find(mboxId);
                    if (storeIt == Store.end()) {
                        continue;
                    }

                    TEventsList events;
                    storeIt->second->Capture(events);
                    Runtime.GetMailbox(edgeActor.NodeId(), edgeActor.Hint()).PushFront(events);
                }
            }

            TTestActorRuntimeBase& Runtime;
            TEventMailBoxList Store;
            bool HasEvents;
        };

        TEventMailBoxList restrictedMailboxes;
        const bool useRestrictedMailboxes = !options.OnlyMailboxes.empty();
        for (auto mailboxId : options.OnlyMailboxes) {
            auto it = Mailboxes.find(mailboxId);
            if (it == Mailboxes.end()) {
                it = Mailboxes.insert(std::make_pair(mailboxId, new TEventMailBox())).first;
            }

            restrictedMailboxes.insert(std::make_pair(mailboxId, it->second));
        }

        TAutoPtr<TTempEdgeEventsCaptor> tempEdgeEventsCaptor;
        if (!restrictedMailboxes) {
            tempEdgeEventsCaptor.Reset(new TTempEdgeEventsCaptor(*this));
        }

        TEventMailBoxList& currentMailboxes = useRestrictedMailboxes ? restrictedMailboxes : Mailboxes;
        while (!currentMailboxes.empty()) {
            bool hasProgress = true;
            while (hasProgress) {
                ++DispatchCyclesCount;
                hasProgress = false;

                ui64 eventsToDispatch = 0;
                for (auto mboxIt = currentMailboxes.begin(); mboxIt != currentMailboxes.end(); ++mboxIt) {
                    if (mboxIt->second->IsActive(TInstant::MicroSeconds(CurrentTimestamp))) {
                        eventsToDispatch += mboxIt->second->GetSentEventCount();
                    }
                }
                ui32 eventsDispatched = 0;

                //TODO: count events before each cycle, break after dispatching that much events
                bool isEmpty = false;
                while (!isEmpty && eventsDispatched < eventsToDispatch) {
                    ui64 mailboxCount = currentMailboxes.size();
                    ui64 startWith = mailboxCount ? DispatcherRandomProvider->GenRand64() % mailboxCount : 0ull;
                    auto startWithMboxIt = currentMailboxes.begin();
                        for (ui64 i = 0; i < startWith; ++i) {
                            ++startWithMboxIt;
                        }
                    auto endWithMboxIt = startWithMboxIt;

                    isEmpty = true;
                    auto mboxIt = startWithMboxIt;
                    TDeque<TEventMailboxId> suspectedBoxes;
                    while (true) {
                        auto& mbox = *mboxIt;
                        bool isIgnored = true;
                        if (!mbox.second->IsEmpty()) {
                            HandleNonEmptyMailboxesForEachContext(mbox.first);
                            if (mbox.second->IsActive(TInstant::MicroSeconds(CurrentTimestamp))) {

                                bool isEdgeMailbox = false;
                                if (EdgeActorByMailbox.FindPtr(TEventMailboxId(mbox.first.NodeId, mbox.first.Hint))) {
                                    isEdgeMailbox = true;
                                    TEventsList events;
                                    mbox.second->Capture(events);

                                    TEventsList eventsToPush;
                                    for (auto& ev : events) {
                                        TInverseGuard<TMutex> inverseGuard(Mutex);

                                        for (auto& observer : ObserverFuncs) {
                                            observer(ev);
                                            if (!ev) break;
                                        }

                                        if (ev && ObserverFunc(ev) != EEventAction::DROP) {
                                            eventsToPush.push_back(ev);
                                        }
                                    }
                                    mbox.second->PushFront(eventsToPush);
                                }

                                if (!isEdgeMailbox) {
                                    isEmpty = false;
                                    isIgnored = false;
                                    ++eventsDispatched;
                                    ++DispatchedEventsCount;
                                    if (DispatchedEventsCount > DispatchedEventsLimit) {
                                        ythrow TWithBackTrace<yexception>() << "Dispatched "
                                            << DispatchedEventsLimit << " events, limit reached.";
                                    }

                                    auto ev = mbox.second->Pop();
                                    if (BlockedOutput.find(ev->Sender) == BlockedOutput.end()) {
                                        //UpdateCurrentTime(TInstant::MicroSeconds(CurrentTimestamp + 10));
                                        if (verbose) {
                                            Cerr << "Process event at " << TInstant::MicroSeconds(CurrentTimestamp) << ", ";
                                            PrintEvent(ev, this);
                                        }
                                    }

                                    hasProgress = true;
                                    EEventAction action;
                                    {
                                        TInverseGuard<TMutex> inverseGuard(Mutex);

                                        for (auto& observer : ObserverFuncs) {
                                            observer(ev);
                                            if (!ev) break;
                                        }

                                        if (ev) {
                                            action = ObserverFunc(ev);
                                        } else {
                                            action = EEventAction::DROP;
                                        }
                                    }

                                    switch (action) {
                                        case EEventAction::PROCESS:
                                            UpdateFinalEventsStatsForEachContext(*ev);
                                            SendInternal(ev.Release(), mbox.first.NodeId - FirstNodeId, false);
                                            break;
                                        case EEventAction::DROP:
                                            // do nothing
                                            break;
                                        case EEventAction::RESCHEDULE: {
                                            TInstant deadline = TInstant::MicroSeconds(CurrentTimestamp) + ReschedulingDelay;
                                            mbox.second->Freeze(deadline);
                                            mbox.second->PushFront(ev);
                                            break;
                                        }
                                        default:
                                            Y_ABORT("Unknown action");
                                    }
                                }
                            }

                        }
                        Y_ABORT_UNLESS(mboxIt != currentMailboxes.end());
                        if (!isIgnored && !CurrentDispatchContext->PrevContext && !restrictedMailboxes &&
                                mboxIt->second->IsEmpty() &&
                                mboxIt->second->IsScheduledEmpty() &&
                                mboxIt->second->IsActive(TInstant::MicroSeconds(CurrentTimestamp))) {
                            suspectedBoxes.push_back(mboxIt->first);
                        }
                        ++mboxIt;
                        if (mboxIt == currentMailboxes.end()) {
                            mboxIt = currentMailboxes.begin();
                        }
                        Y_ABORT_UNLESS(endWithMboxIt != currentMailboxes.end());
                        if (mboxIt == endWithMboxIt) {
                            break;
                        }
                    }

                    for (auto id : suspectedBoxes) {
                        auto it = currentMailboxes.find(id);
                        if (it != currentMailboxes.end() && it->second->IsEmpty() && it->second->IsScheduledEmpty() &&
                                it->second->IsActive(TInstant::MicroSeconds(CurrentTimestamp))) {
                            currentMailboxes.erase(it);
                        }
                    }
                }
            }

            if (localContext.FinalEventFound) {
                return true;
            }

            if (!localContext.FoundNonEmptyMailboxes.empty())
                return true;

            if (options.CustomFinalCondition && options.CustomFinalCondition())
                return true;

            if (options.FinalEvents.empty()) {
                for (auto& mbox : currentMailboxes) {
                    if (!mbox.second->IsActive(TInstant::MicroSeconds(CurrentTimestamp)))
                        continue;

                    if (!mbox.second->IsEmpty()) {
                        if (verbose) {
                            Cerr << "Dispatch complete with non-empty queue at " << TInstant::MicroSeconds(CurrentTimestamp) << "\n";
                        }

                        return true;
                    }
                }
            }

            if (TInstant::MicroSeconds(CurrentTimestamp) > simDeadline) {
                return false;
            }

            if (dispatchTime >= deadline) {
                if (verbose) {
                    Cerr << "Reach deadline at " << TInstant::MicroSeconds(CurrentTimestamp) << "\n";
                }

                ythrow TWithBackTrace<TEmptyEventQueueException>();
            }

            if (!options.Quiet && dispatchTime >= inspectScheduledEventsAt) {
                inspectScheduledEventsAt = dispatchTime + scheduledEventsInspectInterval;
                bool isEmpty = true;
                TMaybe<TInstant> nearestMailboxDeadline;
                TVector<TIntrusivePtr<TEventMailBox>> nextScheduleMboxes;
                TMaybe<TInstant> nextScheduleDeadline;
                for (auto& mbox : currentMailboxes) {
                    if (!mbox.second->IsActive(TInstant::MicroSeconds(CurrentTimestamp))) {
                        if (!nearestMailboxDeadline.Defined() || *nearestMailboxDeadline.Get() > mbox.second->GetInactiveUntil()) {
                            nearestMailboxDeadline = mbox.second->GetInactiveUntil();
                        }

                        continue;
                    }

                    if (mbox.second->IsScheduledEmpty())
                        continue;

                    auto firstScheduleDeadline = mbox.second->GetFirstScheduleDeadline();
                    if (!nextScheduleDeadline || firstScheduleDeadline < *nextScheduleDeadline) {
                        nextScheduleMboxes.clear();
                        nextScheduleMboxes.emplace_back(mbox.second);
                        nextScheduleDeadline = firstScheduleDeadline;
                    } else if (firstScheduleDeadline == *nextScheduleDeadline) {
                        nextScheduleMboxes.emplace_back(mbox.second);
                    }
                }

                for (const auto& nextScheduleMbox : nextScheduleMboxes) {
                    TEventsList selectedEvents;
                    TScheduledEventsList capturedScheduledEvents;
                    nextScheduleMbox->CaptureScheduled(capturedScheduledEvents);
                    ScheduledEventsSelectorFunc(*this, capturedScheduledEvents, selectedEvents);
                    nextScheduleMbox->PushScheduled(capturedScheduledEvents);
                    for (auto& event : selectedEvents) {
                        if (verbose && (BlockedOutput.find(event->Sender) == BlockedOutput.end())) {
                            Cerr << "Selected scheduled event at " << TInstant::MicroSeconds(CurrentTimestamp) << ", ";
                            PrintEvent(event, this);
                        }

                        nextScheduleMbox->Send(event);
                        isEmpty = false;
                    }
                }

                if (!isEmpty) {
                    if (verbose) {
                        Cerr << "Process selected events at " << TInstant::MicroSeconds(CurrentTimestamp) << "\n";
                    }

                    deadline = dispatchTime +  DispatchTimeout;
                    continue;
                }

                if (nearestMailboxDeadline.Defined()) {
                    if (verbose) {
                        Cerr << "Forward time to " << *nearestMailboxDeadline.Get() << "\n";
                    }

                    UpdateCurrentTime(*nearestMailboxDeadline.Get());
                    continue;
                }
            }

            TDuration waitDelay = TDuration::MilliSeconds(10);
            dispatchTime += waitDelay;
            MailboxesHasEvents.WaitT(Mutex, waitDelay);
        }
        return false;
    }

    void TTestActorRuntimeBase::HandleNonEmptyMailboxesForEachContext(TEventMailboxId mboxId) {
        TDispatchContext* context = CurrentDispatchContext;
        while (context) {
            const auto& nonEmptyMailboxes = context->Options->NonEmptyMailboxes;
            if (Find(nonEmptyMailboxes.begin(), nonEmptyMailboxes.end(), mboxId) != nonEmptyMailboxes.end()) {
                context->FoundNonEmptyMailboxes.insert(mboxId);
            }

            context = context->PrevContext;
        }
    }

    void TTestActorRuntimeBase::UpdateFinalEventsStatsForEachContext(IEventHandle& ev) {
        TDispatchContext* context = CurrentDispatchContext;
        while (context) {
            for (const auto& finalEvent : context->Options->FinalEvents) {
                if (finalEvent.EventCheck(ev)) {
                    auto& freq = context->FinalEventFrequency[&finalEvent];
                    if (++freq >= finalEvent.RequiredCount) {
                        context->FinalEventFound = true;
                    }
                }
            }

            context = context->PrevContext;
        }
    }

    void TTestActorRuntimeBase::Send(const TActorId& recipient, const TActorId& sender, TAutoPtr<IEventBase> ev, ui32 senderNodeIndex, bool viaActorSystem) {
        Send(new IEventHandle(recipient, sender, ev.Release()), senderNodeIndex, viaActorSystem);
    }

    void TTestActorRuntimeBase::Send(TAutoPtr<IEventHandle> ev, ui32 senderNodeIndex, bool viaActorSystem) {
        TGuard<TMutex> guard(Mutex);
        Y_ABORT_UNLESS(senderNodeIndex < NodeCount, "senderNodeIndex# %" PRIu32 " < NodeCount# %" PRIu32,
            senderNodeIndex, NodeCount);
        SendInternal(ev, senderNodeIndex, viaActorSystem);
    }

    void TTestActorRuntimeBase::SendAsync(TAutoPtr<IEventHandle> ev, ui32 senderNodeIndex) {
        Send(ev, senderNodeIndex, true);
    }

    void TTestActorRuntimeBase::Schedule(TAutoPtr<IEventHandle> ev, const TDuration& duration, ui32 nodeIndex) {
        TGuard<TMutex> guard(Mutex);
        Y_ABORT_UNLESS(nodeIndex < NodeCount);
        ui32 nodeId = FirstNodeId + nodeIndex;
        ui32 mailboxHint = ev->GetRecipientRewrite().Hint();
        TInstant deadline = TInstant::MicroSeconds(CurrentTimestamp) + duration;
        GetMailbox(nodeId, mailboxHint).Schedule(TScheduledEventQueueItem(deadline, ev, nullptr));
        if (VERBOSE)
            Cerr << "Event was added to scheduled queue\n";
    }

    void TTestActorRuntimeBase::ClearCounters() {
        TGuard<TMutex> guard(Mutex);
        EvCounters.clear();
    }

    ui64 TTestActorRuntimeBase::GetCounter(ui32 evType) const {
        TGuard<TMutex> guard(Mutex);
        auto it = EvCounters.find(evType);
        if (it == EvCounters.end())
            return 0;

        return it->second;
    }

    TActorId TTestActorRuntimeBase::GetLocalServiceId(const TActorId& serviceId, ui32 nodeIndex) {
        TGuard<TMutex> guard(Mutex);
        Y_ABORT_UNLESS(nodeIndex < NodeCount);
        TNodeDataBase* node = Nodes[FirstNodeId + nodeIndex].Get();
        return node->ActorSystem->LookupLocalService(serviceId);
    }

    void TTestActorRuntimeBase::WaitForEdgeEvents(TEventFilter filter, const TSet<TActorId>& edgeFilter, TDuration simTimeout) {
        TGuard<TMutex> guard(Mutex);
        ui32 dispatchCount = 0;
        if (!edgeFilter.empty()) {
            for (auto edgeActor : edgeFilter) {
                Y_ABORT_UNLESS(EdgeActors.contains(edgeActor), "%s is not an edge actor", ToString(edgeActor).data());
            }
        }
        const TSet<TActorId>& edgeActors = edgeFilter.empty() ? EdgeActors : edgeFilter;
        TInstant deadline = TInstant::MicroSeconds(CurrentTimestamp) + simTimeout;
        for (;;) {
            for (auto edgeActor : edgeActors) {
                TEventsList events;
                auto& mbox = GetMailbox(edgeActor.NodeId(), edgeActor.Hint());
                bool foundEvent = false;
                mbox.Capture(events);
                for (auto& ev : events) {
                    if (filter(*this, ev)) {
                        foundEvent = true;
                        break;
                    }
                }

                mbox.PushFront(events);
                if (foundEvent)
                    return;
            }

            ++dispatchCount;
            {
                if (!DispatchEventsInternal(TDispatchOptions(), deadline)) {
                    return; // Timed out; event was not found
                }
            }

            Y_ABORT_UNLESS(dispatchCount < 1000, "Hard limit to prevent endless loop");
        }
    }

    TActorId TTestActorRuntimeBase::GetInterconnectProxy(ui32 nodeIndexFrom, ui32 nodeIndexTo) {
        TGuard<TMutex> guard(Mutex);
        Y_ABORT_UNLESS(nodeIndexFrom < NodeCount);
        Y_ABORT_UNLESS(nodeIndexTo < NodeCount);
        Y_ABORT_UNLESS(nodeIndexFrom != nodeIndexTo);
        TNodeDataBase* node = Nodes[FirstNodeId + nodeIndexFrom].Get();
        return node->ActorSystem->InterconnectProxy(FirstNodeId + nodeIndexTo);
    }

    void TTestActorRuntimeBase::BlockOutputForActor(const TActorId& actorId) {
        TGuard<TMutex> guard(Mutex);
        BlockedOutput.insert(actorId);
    }

    void TTestActorRuntimeBase::SetDispatcherRandomSeed(TInstant time, ui64 iteration) {
        ui64 days = (time.Hours() / 24);
        DispatcherRandomSeed = (days << 32) ^ iteration;
        DispatcherRandomProvider = CreateDeterministicRandomProvider(DispatcherRandomSeed);
    }

    IActor* TTestActorRuntimeBase::FindActor(const TActorId& actorId, ui32 nodeIndex) const {
        TGuard<TMutex> guard(Mutex);
        if (nodeIndex == Max<ui32>()) {
            Y_ABORT_UNLESS(actorId.NodeId());
            nodeIndex = actorId.NodeId() - FirstNodeId;
        }

        Y_ABORT_UNLESS(nodeIndex < NodeCount);
        auto nodeIt = Nodes.find(FirstNodeId + nodeIndex);
        Y_ABORT_UNLESS(nodeIt != Nodes.end());
        TNodeDataBase* node = nodeIt->second.Get();
        return FindActor(actorId, node);
    }

    TStringBuf TTestActorRuntimeBase::FindActorName(const TActorId& actorId, ui32 nodeIndex) const {
        auto actor = FindActor(actorId, nodeIndex);
        if (!actor) {
            return {};
        }
        return TLocalProcessKeyState<TActorActivityTag>::GetInstance().GetNameByIndex(actor->GetActivityType());
    }

    void TTestActorRuntimeBase::EnableScheduleForActor(const TActorId& actorId, bool allow) {
        TGuard<TMutex> guard(Mutex);
        if (allow) {
            if (VERBOSE) {
                Cerr << "Actor " << actorId << " added to schedule whitelist";
            }
            ScheduleWhiteList.insert(actorId);
        } else {
            if (VERBOSE) {
                Cerr << "Actor " << actorId << " removed from schedule whitelist";
            }
            ScheduleWhiteList.erase(actorId);
        }
    }

    bool TTestActorRuntimeBase::IsScheduleForActorEnabled(const TActorId& actorId) const {
        TGuard<TMutex> guard(Mutex);
        return ScheduleWhiteList.find(actorId) != ScheduleWhiteList.end();
    }

    TIntrusivePtr<NMonitoring::TDynamicCounters> TTestActorRuntimeBase::GetDynamicCounters(ui32 nodeIndex) {
        TGuard<TMutex> guard(Mutex);
        Y_ABORT_UNLESS(nodeIndex < NodeCount);
        ui32 nodeId = FirstNodeId + nodeIndex;
        TNodeDataBase* node = Nodes[nodeId].Get();
        return node->DynamicCounters;
    }

    void TTestActorRuntimeBase::SetupMonitoring(ui16 monitoringPortOffset, bool monitoringTypeAsync) {
        NeedMonitoring = true;
        MonitoringPortOffset = monitoringPortOffset;
        MonitoringTypeAsync = monitoringTypeAsync;
    }

    void TTestActorRuntimeBase::SendInternal(TAutoPtr<IEventHandle> ev, ui32 nodeIndex, bool viaActorSystem) {
        Y_ABORT_UNLESS(nodeIndex < NodeCount);
        ui32 nodeId = FirstNodeId + nodeIndex;
        TNodeDataBase* node = Nodes[nodeId].Get();
        ui32 targetNode = ev->GetRecipientRewrite().NodeId();
        ui32 targetNodeIndex;
        if (targetNode == 0) {
            targetNodeIndex = nodeIndex;
        } else {
            targetNodeIndex = targetNode - FirstNodeId;
            Y_ABORT_UNLESS(targetNodeIndex < NodeCount);
        }

        if (viaActorSystem || UseRealThreads || ev->GetRecipientRewrite().IsService() || (targetNodeIndex != nodeIndex)) {
            node->ActorSystem->Send(ev);
            return;
        }

        Y_ABORT_UNLESS(!ev->GetRecipientRewrite().IsService() && (targetNodeIndex == nodeIndex));

        if (!AllowSendFrom(node, ev)) {
            return;
        }

        ui32 mailboxHint = ev->GetRecipientRewrite().Hint();
        TEventMailBox& mbox = GetMailbox(nodeId, mailboxHint);
        if (!mbox.IsActive(TInstant::MicroSeconds(CurrentTimestamp))) {
            mbox.PushFront(ev);
            return;
        }

        ui64 recipientLocalId = ev->GetRecipientRewrite().LocalId();
        if ((BlockedOutput.find(ev->Sender) == BlockedOutput.end()) && VERBOSE) {
            Cerr << "Send event, ";
            PrintEvent(ev, this);
        }

        EvCounters[ev->GetTypeRewrite()]++;

        TMailboxHeader* mailbox = node->MailboxTable->Get(mailboxHint);
        IActor* recipientActor = mailbox->FindActor(recipientLocalId);
        if (recipientActor) {
            // Save actorId by value in order to prevent ctx from being invalidated during another Send call.
            TActorId actorId = ev->GetRecipientRewrite();
            node->ActorToActorId[recipientActor] = ev->GetRecipientRewrite();
            TActorContext ctx(*mailbox, *node->ExecutorThread, GetCycleCountFast(), actorId);
            TActivationContext *prevTlsActivationContext = TlsActivationContext;
            TlsActivationContext = &ctx;
            CurrentRecipient = actorId;
            {
                TInverseGuard<TMutex> inverseGuard(Mutex);
#ifdef USE_ACTOR_CALLSTACK
                TCallstack::GetTlsCallstack() = ev->Callstack;
                TCallstack::GetTlsCallstack().SetLinesToSkip();
#endif
                recipientActor->Receive(ev);
                node->ExecutorThread->DropUnregistered();
            }
            CurrentRecipient = TActorId();
            TlsActivationContext = prevTlsActivationContext;
        } else {
            if (VERBOSE) {
                Cerr << "Failed to find actor with local id: " << recipientLocalId << "\n";
            }

            auto fw = IEventHandle::ForwardOnNondelivery(ev, TEvents::TEvUndelivered::ReasonActorUnknown);
            node->ActorSystem->Send(fw);
        }
    }

    IActor* TTestActorRuntimeBase::FindActor(const TActorId& actorId, TNodeDataBase* node) const {
        ui32 mailboxHint = actorId.Hint();
        ui64 localId = actorId.LocalId();
        TMailboxHeader* mailbox = node->MailboxTable->Get(mailboxHint);
        IActor* actor = mailbox->FindActor(localId);
        return actor;
    }

    THolder<TActorSystemSetup> TTestActorRuntimeBase::MakeActorSystemSetup(ui32 nodeIndex, TNodeDataBase* node) {
        THolder<TActorSystemSetup> setup(new TActorSystemSetup);
        setup->NodeId = FirstNodeId + nodeIndex;

        IHarmonizer* harmonizer = nullptr;
        if (node) {
            node->Harmonizer.reset(MakeHarmonizer(GetCycleCountFast()));
            harmonizer = node->Harmonizer.get();
        }

        if (UseRealThreads) {
            setup->ExecutorsCount = 5;
            setup->Executors.Reset(new TAutoPtr<IExecutorPool>[5]);
            setup->Executors[0].Reset(new TBasicExecutorPool(0, 2, 20, "System", harmonizer));
            setup->Executors[1].Reset(new TBasicExecutorPool(1, 2, 20, "User", harmonizer));
            setup->Executors[2].Reset(new TIOExecutorPool(2, 1, "IO"));
            setup->Executors[3].Reset(new TBasicExecutorPool(3, 2, 20, "Batch", harmonizer));
            setup->Executors[4].Reset(new TBasicExecutorPool(4, 1, 20, "IC", harmonizer));
            setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig(512, 100)));
        } else {
            setup->ExecutorsCount = 1;
            setup->Scheduler.Reset(new TSchedulerThreadStub(this, node));
            setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
            setup->Executors[0].Reset(new TExecutorPoolStub(this, nodeIndex, node, 0));
        }

        InitActorSystemSetup(*setup, node);

        return setup;
    }

    THolder<TActorSystem> TTestActorRuntimeBase::MakeActorSystem(ui32 nodeIndex, TNodeDataBase* node) {
        auto setup = MakeActorSystemSetup(nodeIndex, node);

        node->ExecutorPools.resize(setup->ExecutorsCount);
        for (ui32 i = 0; i < setup->ExecutorsCount; ++i) {
            IExecutorPool* executor = setup->Executors[i].Get();
            node->ExecutorPools[i] = executor;
            node->Harmonizer->AddPool(executor);
        }

        const auto& interconnectCounters = GetCountersForComponent(node->DynamicCounters, "interconnect");

        for (const auto& cmd : node->LocalServices) {
            setup->LocalServices.emplace_back(cmd.first, TActorSetupCmd(cmd.second.Actor, cmd.second.MailboxType, cmd.second.PoolId));
        }
        setup->Interconnect.ProxyActors.resize(FirstNodeId + NodeCount);
        const TActorId nameserviceId = GetNameserviceActorId();

        TIntrusivePtr<TInterconnectProxyCommon> common;
        common.Reset(new TInterconnectProxyCommon);
        common->NameserviceId = nameserviceId;
        common->MonCounters = interconnectCounters;
        common->TechnicalSelfHostName = "::1";

        if (!UseRealThreads) {
            common->Settings.DeadPeer = TDuration::Max();
            common->Settings.CloseOnIdle = TDuration::Max();
            common->Settings.PingPeriod = TDuration::Max();
            common->Settings.ForceConfirmPeriod = TDuration::Max();
            common->Settings.Handshake = TDuration::Max();
        }

        common->ClusterUUID = ClusterUUID;
        common->AcceptUUID = {ClusterUUID};

        if (ICCommonSetupper) {
            ICCommonSetupper(nodeIndex, common);
        }

        for (ui32 proxyNodeIndex = 0; proxyNodeIndex < NodeCount; ++proxyNodeIndex) {
            if (proxyNodeIndex == nodeIndex)
                continue;

            const ui32 peerNodeId = FirstNodeId + proxyNodeIndex;

            IActor *proxyActor = UseRealInterconnect
                ? new TInterconnectProxyTCP(peerNodeId, common)
                : InterconnectMock.CreateProxyMock(setup->NodeId, peerNodeId, common);

            setup->Interconnect.ProxyActors[peerNodeId] = {proxyActor, TMailboxType::ReadAsFilled, InterconnectPoolId()};
        }

        setup->Interconnect.ProxyWrapperFactory = CreateProxyWrapperFactory(common, InterconnectPoolId(), &InterconnectMock);

        if (UseRealInterconnect) {
            setup->LocalServices.emplace_back(MakePollerActorId(), NActors::TActorSetupCmd(CreatePollerActor(),
                NActors::TMailboxType::Simple, InterconnectPoolId()));
        }

        if (!SingleSysEnv) { // Single system env should do this self
            if (LogBackendFactory) {
                LogBackend = LogBackendFactory();
            }
            TAutoPtr<TLogBackend> logBackend = LogBackend ? LogBackend : NActors::CreateStderrBackend();
            NActors::TLoggerActor *loggerActor = new NActors::TLoggerActor(node->LogSettings,
                logBackend, GetCountersForComponent(node->DynamicCounters, "utils"));
            NActors::TActorSetupCmd loggerActorCmd(loggerActor, NActors::TMailboxType::Simple, node->GetLoggerPoolId());
            std::pair<NActors::TActorId, NActors::TActorSetupCmd> loggerActorPair(node->LogSettings->LoggerActorId, std::move(loggerActorCmd));
            setup->LocalServices.push_back(std::move(loggerActorPair));
        }

        return THolder<TActorSystem>(new TActorSystem(setup, node->GetAppData(), node->LogSettings));
    }

    TActorSystem* TTestActorRuntimeBase::SingleSys() const {
        Y_ABORT_UNLESS(Nodes.size() == 1, "Works only for single system env");

        return Nodes.begin()->second->ActorSystem.Get();
    }

    TActorSystem* TTestActorRuntimeBase::GetAnyNodeActorSystem() {
        for (auto& x : Nodes) {
            return x.second->ActorSystem.Get();
        }
        Y_ABORT("Don't use this method.");
    }

    TActorSystem* TTestActorRuntimeBase::GetActorSystem(ui32 nodeId) {
        auto it = Nodes.find(GetNodeId(nodeId));
        Y_ABORT_UNLESS(it != Nodes.end());
        return it->second->ActorSystem.Get();
    }


    TEventMailBox& TTestActorRuntimeBase::GetMailbox(ui32 nodeId, ui32 hint) {
        TGuard<TMutex> guard(Mutex);
        auto mboxId = TEventMailboxId(nodeId, hint);
        auto it = Mailboxes.find(mboxId);
        if (it == Mailboxes.end()) {
            it = Mailboxes.insert(std::make_pair(mboxId, new TEventMailBox())).first;
        }

        return *it->second;
    }

    void TTestActorRuntimeBase::ClearMailbox(ui32 nodeId, ui32 hint) {
        TGuard<TMutex> guard(Mutex);
        auto mboxId = TEventMailboxId(nodeId, hint);
        Mailboxes.erase(mboxId);
    }

    TString TTestActorRuntimeBase::GetActorName(const TActorId& actorId) const {
        auto it = ActorNames.find(actorId);
        if (it != ActorNames.end())
            return it->second;
        return actorId.ToString();
    }

    struct TStrandingActorDecoratorContext : public TThrRefBase {
        TStrandingActorDecoratorContext()
            : Queue(new TQueueType)
        {
        }

        typedef TOneOneQueueInplace<IEventHandle*, 32> TQueueType;
        TAutoPtr<TQueueType, TQueueType::TPtrCleanDestructor> Queue;
    };

    class TStrandingActorDecorator : public TActorBootstrapped<TStrandingActorDecorator> {
    public:
        class TReplyActor : public TActor<TReplyActor> {
        public:
            static constexpr EActivityType ActorActivityType() {
                return EActivityType::TEST_ACTOR_RUNTIME;
            }

            TReplyActor(TStrandingActorDecorator* owner)
                : TActor(&TReplyActor::StateFunc)
                , Owner(owner)
            {
            }

            STFUNC(StateFunc);

        private:
            TStrandingActorDecorator* const Owner;
        };

        static constexpr EActivityType ActorActivityType() {
            return EActivityType::TEST_ACTOR_RUNTIME;
        }

        TStrandingActorDecorator(const TActorId& delegatee, bool isSync, const TVector<TActorId>& additionalActors,
            TSimpleSharedPtr<TStrandingActorDecoratorContext> context, TTestActorRuntimeBase* runtime,
            TReplyCheckerCreator createReplyChecker)
            : Delegatee(delegatee)
            , IsSync(isSync)
            , AdditionalActors(additionalActors)
            , Context(context)
            , HasReply(false)
            , Runtime(runtime)
            , ReplyChecker(createReplyChecker())
        {
            if (IsSync) {
                Y_ABORT_UNLESS(!runtime->IsRealThreads());
            }
        }

        void Bootstrap(const TActorContext& ctx) {
            Become(&TStrandingActorDecorator::StateFunc);
            ReplyId = ctx.RegisterWithSameMailbox(new TReplyActor(this));
            DelegateeOptions.OnlyMailboxes.push_back(TEventMailboxId(Delegatee.NodeId(), Delegatee.Hint()));
            for (const auto& actor : AdditionalActors) {
                DelegateeOptions.OnlyMailboxes.push_back(TEventMailboxId(actor.NodeId(), actor.Hint()));
            }

            DelegateeOptions.OnlyMailboxes.push_back(TEventMailboxId(ReplyId.NodeId(), ReplyId.Hint()));
            DelegateeOptions.NonEmptyMailboxes.push_back(TEventMailboxId(ReplyId.NodeId(), ReplyId.Hint()));
            DelegateeOptions.Quiet = true;
        }

        STFUNC(StateFunc) {
            bool wasEmpty = !Context->Queue->Head();
            Context->Queue->Push(ev.Release());
            if (wasEmpty) {
                SendHead(ActorContext());
            }
        }

        STFUNC(Reply) {
            Y_ABORT_UNLESS(!HasReply);
            IEventHandle *requestEv = Context->Queue->Head();
            TActorId originalSender = requestEv->Sender;
            HasReply = !ReplyChecker->IsWaitingForMoreResponses(ev.Get());
            if (HasReply) {
                delete Context->Queue->Pop();
            }
            auto ctx(ActorContext());
            ctx.ExecutorThread.Send(IEventHandle::Forward(ev, originalSender));
            if (!IsSync && Context->Queue->Head()) {
                SendHead(ctx);
            }
        }

    private:
        void SendHead(const TActorContext& ctx) {
            if (!IsSync) {
                ctx.ExecutorThread.Send(GetForwardedEvent().Release());
            } else {
                while (Context->Queue->Head()) {
                    HasReply = false;
                    ctx.ExecutorThread.Send(GetForwardedEvent().Release());
                    int count = 100;
                    while (!HasReply && count > 0) {
                        try {
                            Runtime->DispatchEvents(DelegateeOptions);
                        } catch (TEmptyEventQueueException&) {
                            count--;
                            Cerr << "No reply" << Endl;
                        }
                    }

                    Runtime->UpdateCurrentTime(Runtime->GetCurrentTime() + TDuration::MicroSeconds(1000));
                }
            }
        }

        TAutoPtr<IEventHandle> GetForwardedEvent() {
            IEventHandle* ev = Context->Queue->Head();
            ReplyChecker->OnRequest(ev);
            TAutoPtr<IEventHandle> forwardedEv = ev->HasEvent()
                    ? new IEventHandle(Delegatee, ReplyId, ev->ReleaseBase().Release(), ev->Flags, ev->Cookie)
                    : new IEventHandle(ev->GetTypeRewrite(), ev->Flags, Delegatee, ReplyId, ev->ReleaseChainBuffer(), ev->Cookie);

            return forwardedEv;
        }
    private:
        const TActorId Delegatee;
        const bool IsSync;
        const TVector<TActorId> AdditionalActors;
        TSimpleSharedPtr<TStrandingActorDecoratorContext> Context;
        TActorId ReplyId;
        bool HasReply;
        TDispatchOptions DelegateeOptions;
        TTestActorRuntimeBase* Runtime;
        THolder<IReplyChecker> ReplyChecker;
    };

    void TStrandingActorDecorator::TReplyActor::StateFunc(STFUNC_SIG) {
        Owner->Reply(ev);
    }

    class TStrandingDecoratorFactory : public IStrandingDecoratorFactory {
    public:
        TStrandingDecoratorFactory(TTestActorRuntimeBase* runtime,
            TReplyCheckerCreator createReplyChecker)
            : Context(new TStrandingActorDecoratorContext())
            , Runtime(runtime)
            , CreateReplyChecker(createReplyChecker)
        {
        }

        IActor* Wrap(const TActorId& delegatee, bool isSync, const TVector<TActorId>& additionalActors) override {
            return new TStrandingActorDecorator(delegatee, isSync, additionalActors, Context, Runtime,
                CreateReplyChecker);
        }

    private:
        TSimpleSharedPtr<TStrandingActorDecoratorContext> Context;
        TTestActorRuntimeBase* Runtime;
        TReplyCheckerCreator CreateReplyChecker;
    };

    TAutoPtr<IStrandingDecoratorFactory> CreateStrandingDecoratorFactory(TTestActorRuntimeBase* runtime,
            TReplyCheckerCreator createReplyChecker) {
        return TAutoPtr<IStrandingDecoratorFactory>(new TStrandingDecoratorFactory(runtime, createReplyChecker));
    }

    ui64 DefaultRandomSeed = 9999;
}
