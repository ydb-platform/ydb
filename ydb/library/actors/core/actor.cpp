#include "actor.h"
#include "debug.h"
#include "actorsystem.h"
#include "executor_thread.h"
#include <ydb/library/actors/util/datetime.h>

#define POOL_ID() \
    (!TlsThreadContext ? "OUTSIDE" : \
    (TlsThreadContext->IsShared() ? "Shared[" + ToString(TlsThreadContext->OwnerPoolId()) + "]_" + ToString(TlsThreadContext->PoolId()) : \
    ("Pool_" + ToString(TlsThreadContext->PoolId()))))

#define WORKER_ID() ("Worker_" + ToString(TlsThreadContext ? TlsThreadContext->WorkerId() : Max<TWorkerId>()))

#define ACTOR_DEBUG(level, ...) \
    ACTORLIB_DEBUG(level, POOL_ID(), " ", WORKER_ID(), " ", __func__, ": ", __VA_ARGS__)


namespace NActors {
    Y_POD_THREAD(TThreadContext*) TlsThreadContext(nullptr);
    thread_local TActivationContext *TActivationContextHolder::Value = nullptr;
    TActivationContextHolder TlsActivationContext;

    [[gnu::noinline]] TActivationContextHolder::operator bool() const {
        asm volatile("");
        return Value != nullptr;
    }

    [[gnu::noinline]] TActivationContextHolder::operator TActivationContext*() const {
        asm volatile("");
        return Value;
    }

    [[gnu::noinline]] TActivationContext *TActivationContextHolder::operator ->() {
        asm volatile("");
        return Value;
    }

    [[gnu::noinline]] TActivationContext& TActivationContextHolder::operator *() {
        asm volatile("");
        return *Value;
    }

    [[gnu::noinline]] TActivationContextHolder& TActivationContextHolder::operator =(TActivationContext *context) {
        asm volatile("");
        Value = context;
        return *this;
    }

    static thread_local TActorRunnableQueue* TlsActorRunnableQueue = nullptr;

    TActorRunnableQueue::TActorRunnableQueue(IActor* actor) noexcept {
        Actor_ = actor;
        Prev_ = TlsActorRunnableQueue;
        TlsActorRunnableQueue = this;
    }

    TActorRunnableQueue::~TActorRunnableQueue() {
        Execute();
        TlsActorRunnableQueue = Prev_;
    }

    void TActorRunnableQueue::Schedule(TActorRunnableItem* item) noexcept {
        TActorRunnableQueue* queue = TlsActorRunnableQueue;
        Y_ABORT_UNLESS(queue, "Trying to schedule actor runnable outside an event handler");
        queue->Queue_.PushBack(item);
    }

    void TActorRunnableQueue::Cancel(TActorRunnableItem* item) noexcept {
        item->Unlink();
    }

    void TActorRunnableQueue::Execute() noexcept {
        while (!Queue_.Empty()) {
            TActorRunnableItem* item = Queue_.PopFront();
            item->Run(Actor_);
        }
    }

    void IActor::Describe(IOutputStream &out) const {
        SelfActorId.Out(out);
    }

    bool IActor::Send(TAutoPtr<IEventHandle> ev) const noexcept {
        return TActivationContext::Send(ev);
    }

    bool IActor::Send(const TActorId& recipient, IEventBase* ev, ui32 flags, ui64 cookie, NWilson::TTraceId traceId) const noexcept {
        return SelfActorId.Send(recipient, ev, flags, cookie, std::move(traceId));
    }

    void TActivationContext::Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie) {
        TlsActivationContext->ExecutorThread.Schedule(deadline, ev, cookie);
    }

    void TActivationContext::Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie) {
        TlsActivationContext->ExecutorThread.Schedule(deadline, ev, cookie);
    }

    void TActivationContext::Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie) {
        TlsActivationContext->ExecutorThread.Schedule(delta, ev, cookie);
    }

    bool TActivationContext::Send(const TActorId& recipient, std::unique_ptr<IEventBase> ev, ui32 flags, ui64 cookie) {
        return ActorSystem()->Send(recipient, ev.release(), flags, cookie);
    }

    void TActorIdentity::Schedule(TInstant deadline, IEventBase* ev, ISchedulerCookie* cookie) const {
        return TActivationContext::Schedule(deadline, new IEventHandle(*this, {}, ev), cookie);
    }

    void TActorIdentity::Schedule(TMonotonic deadline, IEventBase* ev, ISchedulerCookie* cookie) const {
        return TActivationContext::Schedule(deadline, new IEventHandle(*this, {}, ev), cookie);
    }

    void TActorIdentity::Schedule(TDuration delta, IEventBase* ev, ISchedulerCookie* cookie) const {
        return TActivationContext::Schedule(delta, new IEventHandle(*this, {}, ev), cookie);
    }

    TActorId TActivationContext::RegisterWithSameMailbox(IActor* actor, TActorId parentId) {
        Y_DEBUG_ABORT_UNLESS(parentId);
        auto& ctx = *TlsActivationContext;
        return ctx.ExecutorThread.RegisterActor(actor, &ctx.Mailbox, parentId);
    }

    TActorId TActorContext::RegisterWithSameMailbox(IActor* actor) const {
        return ExecutorThread.RegisterActor(actor, &Mailbox, SelfID);
    }

    TActorId IActor::RegisterWithSameMailbox(IActor* actor) const noexcept {
        return TlsActivationContext->ExecutorThread.RegisterActor(actor, &TlsActivationContext->Mailbox, SelfActorId);
    }

    TActorId IActor::RegisterAlias() noexcept {
        return TlsActivationContext->ExecutorThread.RegisterAlias(&TlsActivationContext->Mailbox, this);
    }

    void IActor::UnregisterAlias(const TActorId& actorId) noexcept {
        return TlsActivationContext->ExecutorThread.UnregisterAlias(&TlsActivationContext->Mailbox, actorId);
    }

    TActorId TActivationContext::InterconnectProxy(ui32 destinationNodeId) {
        return TlsActivationContext->ExecutorThread.ActorSystem->InterconnectProxy(destinationNodeId);
    }

    TActorSystem* TActivationContext::ActorSystem() {
        return TlsActivationContext->ExecutorThread.ActorSystem;
    }

    i64 TActivationContext::GetCurrentEventTicks() {
        return GetCycleCountFast() - TlsActivationContext->EventStart;
    }

    double TActivationContext::GetCurrentEventTicksAsSeconds() {
        return NHPTimer::GetSeconds(GetCurrentEventTicks());
    }

    void TActivationContext::EnableMailboxStats() {
        TlsActivationContext->Mailbox.EnableStats();
    }

    TActorId IActor::Register(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId) const noexcept {
        return TlsActivationContext->ExecutorThread.RegisterActor(actor, mailboxType, poolId, SelfActorId);
    }

    void TActorContext::Schedule(TInstant deadline, IEventBase* ev, ISchedulerCookie* cookie) const {
        ExecutorThread.Schedule(deadline, new IEventHandle(SelfID, TActorId(), ev), cookie);
    }

    void TActorContext::Schedule(TMonotonic deadline, IEventBase* ev, ISchedulerCookie* cookie) const {
        ExecutorThread.Schedule(deadline, new IEventHandle(SelfID, TActorId(), ev), cookie);
    }

    void TActorContext::Schedule(TDuration delta, IEventBase* ev, ISchedulerCookie* cookie) const {
        ExecutorThread.Schedule(delta, new IEventHandle(SelfID, TActorId(), ev), cookie);
    }

    void TActorContext::Schedule(TInstant deadline, std::unique_ptr<IEventHandle> ev, ISchedulerCookie* cookie) const {
        ExecutorThread.Schedule(deadline, ev.release(), cookie);
    }

    void TActorContext::Schedule(TMonotonic deadline, std::unique_ptr<IEventHandle> ev, ISchedulerCookie* cookie) const {
        ExecutorThread.Schedule(deadline, ev.release(), cookie);
    }

    void TActorContext::Schedule(TDuration delta, std::unique_ptr<IEventHandle> ev, ISchedulerCookie* cookie) const {
        ExecutorThread.Schedule(delta, ev.release(), cookie);
    }

    void IActor::Schedule(TInstant deadline, IEventBase* ev, ISchedulerCookie* cookie) const noexcept {
        TlsActivationContext->ExecutorThread.Schedule(deadline, new IEventHandle(SelfActorId, TActorId(), ev), cookie);
    }

    void IActor::Schedule(TMonotonic deadline, IEventBase* ev, ISchedulerCookie* cookie) const noexcept {
        TlsActivationContext->ExecutorThread.Schedule(deadline, new IEventHandle(SelfActorId, TActorId(), ev), cookie);
    }

    void IActor::Schedule(TDuration delta, IEventBase* ev, ISchedulerCookie* cookie) const noexcept {
        TlsActivationContext->ExecutorThread.Schedule(delta, new IEventHandle(SelfActorId, TActorId(), ev), cookie);
    }

    TInstant TActivationContext::Now() {
        return TlsActivationContext->ExecutorThread.ActorSystem->Timestamp();
    }

    TMonotonic TActivationContext::Monotonic() {
        return TlsActivationContext->ExecutorThread.ActorSystem->Monotonic();
    }

    TInstant TActorContext::Now() const {
        return ExecutorThread.ActorSystem->Timestamp();
    }

    TMonotonic TActorContext::Monotonic() const {
        return ExecutorThread.ActorSystem->Monotonic();
    }

    NLog::TSettings* TActivationContext::LoggerSettings() const {
        return ExecutorThread.ActorSystem->LoggerSettings();
    }

    std::pair<ui32, ui32> TActorContext::CountMailboxEvents(ui32 maxTraverse) const {
        return Mailbox.CountMailboxEvents(SelfID.LocalId(), maxTraverse);
    }

    std::pair<ui32, ui32> IActor::CountMailboxEvents(ui32 maxTraverse) const {
        return TlsActivationContext->Mailbox.CountMailboxEvents(SelfActorId.LocalId(), maxTraverse);
    }

    void IActor::Die(const TActorContext& ctx) {
        if (ctx.SelfID)
            Y_ABORT_UNLESS(ctx.SelfID == SelfActorId);
        PassAway();
    }

    struct TSentinelActorTask : public TActorTask {
        void Cancel() noexcept override {};
        void Destroy() noexcept override {};
    };

    void IActor::PassAway() {
        Y_ABORT_UNLESS(!PassedAway, "Actors must never call PassAway more than once");
        PassedAway = true;

        if (!ActorTasks.Empty()) {
            TSentinelActorTask sentinel;
            ActorTasks.PushBack(&sentinel);
            for (;;) {
                TActorTask* task = ActorTasks.PopFront();
                if (task == &sentinel) {
                    break;
                }
                ActorTasks.PushBack(task);
                task->Cancel();
            }
            // Wait until all actor tasks have finished
            if (!ActorTasks.Empty()) {
                return;
            }
        }

        FinishPassAway();
    }

    void IActor::FinishPassAway() {
        auto& cx = *TlsActivationContext;
        cx.ExecutorThread.UnregisterActor(&cx.Mailbox, SelfActorId);
    }

    void IActor::DestroyActorTasks() {
        if (!ActorTasks.Empty()) {
            TActorRunnableQueue queue(this);
            while (!ActorTasks.Empty()) {
                TActorTask* task = ActorTasks.PopFront();
                task->Destroy();
            }
        }
    }

    bool IActor::RegisterActorTask(TActorTask* task) {
        Y_ABORT_UNLESS(!PassedAway || !ActorTasks.Empty(), "Starting new tasks after actor dies is not allowed");
        ActorTasks.PushBack(task);
        return !PassedAway;
    }

    void IActor::RegisterEventAwaiter(ui64 cookie, TActorEventAwaiter* awaiter) {
        EventAwaiters[cookie].PushBack(awaiter);
    }

    void IActor::UnregisterEventAwaiter(ui64 cookie, TActorEventAwaiter* awaiter) {
        auto it = EventAwaiters.find(cookie);
        if (it != EventAwaiters.end()) {
            it->second.Remove(awaiter);
            if (it->second.Empty()) {
                EventAwaiters.erase(it);
            }
        }
    }

    void IActor::UnregisterActorTask(TActorTask* task) {
        if (task->Empty()) {
            // Task is not in the list
            return;
        }
        ActorTasks.Remove(task);
        if (ActorTasks.Empty() && PassedAway) {
            FinishPassAway();
        }
    }

    double IActor::GetElapsedTicksAsSeconds() const {
        return NHPTimer::GetSeconds(ElapsedTicks);
    }

    bool IActor::HandleResumeRunnable(TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvents::TSystem::ResumeRunnable) {
            auto* msg = ev->Get<TEvents::TEvResumeRunnable>();
            auto* item = msg->Item;
            if (item != nullptr) {
                msg->Item = nullptr;
                item->Run(this);
            }
            return true;
        }
        return false;
    }

    bool IActor::HandleRegisteredEvent(TAutoPtr<IEventHandle>& ev) {
        if (!EventAwaiters.empty()) {
            auto it = EventAwaiters.find(ev->Cookie);
            if (it != EventAwaiters.end()) {
                for (auto& awaiter : it->second) {
                    if (awaiter.Handle(ev)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    void IActor::Receive(TAutoPtr<IEventHandle>& ev) {
#ifndef NDEBUG
        if (ev->Flags & IEventHandle::FlagDebugTrackReceive) {
            YaDebugBreak();
        }
#endif
        ++HandledEvents;
        LastReceiveTimestamp = TActivationContext::Monotonic();

        TActorRunnableQueue queue(this);

        try {
            if (!HandleResumeRunnable(ev) && !HandleRegisteredEvent(ev)) {
                (this->*StateFunc_)(ev);
            }
        } catch (...) {
            if (!OnUnhandledExceptionSafe(std::current_exception())) {
                throw;
            }
        }
    }

    bool IActor::OnUnhandledExceptionSafe(const std::exception_ptr& excPtr) {
        auto* handler = dynamic_cast<IActorExceptionHandler*>(this);
        if (!handler) {
            return false;
        }

        try {
            return handler->OnUnhandledException(excPtr);
        } catch (const std::exception& handleExc) {
            Cerr << "OnUnhandledException throws unhandled exception "
                << TypeName(handleExc) << ": " << handleExc.what() << Endl
                << TBackTrace::FromCurrentException().PrintToString()
                << Endl;
            return false;
        }
    }

    void IActor::Registered(TActorSystem* sys, const TActorId& owner) {
        // fallback to legacy method, do not use it anymore
        if (auto eh = AfterRegister(SelfId(), owner)) {
            if (!TlsThreadContext || TlsThreadContext->CheckSendingType(ESendingType::Common)) {
                sys->Send(eh);
            } else {
                sys->SpecificSend(std::unique_ptr<IEventHandle>(eh.Release()));
            }
        }
    }

    void IActor::SetEnoughCpu(bool isEnough) {
        if (TlsThreadContext) {
            TlsThreadContext->IsEnoughCpu = isEnough;
        }
    }

    void IActor::SetActivityType(TActorActivityType activityType) {
        Y_ENSURE(!SelfActorId, "Cannot change activity type for registered actors");
        ActivityType = activityType;
    }

    template bool TExecutorThread::Send<ESendingType::Common>(TAutoPtr<IEventHandle> ev);
    template bool TExecutorThread::Send<ESendingType::Lazy>(TAutoPtr<IEventHandle> ev);
    template bool TExecutorThread::Send<ESendingType::Tail>(TAutoPtr<IEventHandle> ev);

    template <ESendingType SendingType>
    bool TExecutorThread::Send(TAutoPtr<IEventHandle> ev) {
#ifdef USE_ACTOR_CALLSTACK
        do {
            (ev)->Callstack = TCallstack::GetTlsCallstack();
            (ev)->Callstack.Trace();
        } while (false)
#endif
        ExecutionStats.IncrementSentEvents();
        return ActorSystem->Send<SendingType>(ev);
    }

    template TActorId TExecutorThread::RegisterActor<ESendingType::Common>(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId,
            TActorId parentId);
    template TActorId TExecutorThread::RegisterActor<ESendingType::Lazy>(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId,
            TActorId parentId);
    template TActorId TExecutorThread::RegisterActor<ESendingType::Tail>(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId,
            TActorId parentId);

    template <ESendingType SendingType>
    TActorId TExecutorThread::RegisterActor(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId,
            TActorId parentId)
    {
        if (!parentId) {
            parentId = CurrentRecipient;
        }
        if (poolId == Max<ui32>()) {
            TActorId id;
            if constexpr (SendingType == ESendingType::Common) {
                id = ThreadCtx.Pool()->Register(actor, mailboxType, ++RevolvingWriteCounter, parentId);
            } else if (!TlsThreadContext) {
                id = ThreadCtx.Pool()->Register(actor, mailboxType, ++RevolvingWriteCounter, parentId);
            } else {
                ESendingType previousType = TlsThreadContext->ExchangeSendingType(SendingType);
                id = ThreadCtx.Pool()->Register(actor, mailboxType, ++RevolvingWriteCounter, parentId);
                TlsThreadContext->SetSendingType(previousType);
            }
            return id;
        } else {
            return ActorSystem->Register<SendingType>(actor, mailboxType, poolId, ++RevolvingWriteCounter, parentId);
        }
    }

    template TActorId TExecutorThread::RegisterActor<ESendingType::Common>(IActor* actor, TMailbox* mailbox, TActorId parentId);
    template TActorId TExecutorThread::RegisterActor<ESendingType::Lazy>(IActor* actor, TMailbox* mailbox, TActorId parentId);
    template TActorId TExecutorThread::RegisterActor<ESendingType::Tail>(IActor* actor, TMailbox* mailbox, TActorId parentId);

    template <ESendingType SendingType>
    TActorId TExecutorThread::RegisterActor(IActor* actor, TMailbox* mailbox, TActorId parentId) {
        if (!parentId) {
            parentId = CurrentRecipient;
        }
        if constexpr (SendingType == ESendingType::Common) {
            return ThreadCtx.Pool()->Register(actor, mailbox, parentId);
        } else if (!TlsActivationContext) {
            return ThreadCtx.Pool()->Register(actor, mailbox, parentId);
        } else {
            ESendingType previousType = TlsThreadContext->ExchangeSendingType(SendingType);
            TActorId id = ThreadCtx.Pool()->Register(actor, mailbox, parentId);
            TlsThreadContext->SetSendingType(previousType);
            return id;
        }
    }

    TActorId TExecutorThread::RegisterAlias(TMailbox* mailbox, IActor* actor) {
        return ThreadCtx.Pool()->RegisterAlias(mailbox, actor);
    }

    void TExecutorThread::UnregisterAlias(TMailbox* mailbox, const TActorId& actorId) {
        ThreadCtx.Pool()->UnregisterAlias(mailbox, actorId);
    }

    template bool TActivationContext::Send<ESendingType::Common>(TAutoPtr<IEventHandle> ev);
    template bool TActivationContext::Send<ESendingType::Lazy>(TAutoPtr<IEventHandle> ev);
    template bool TActivationContext::Send<ESendingType::Tail>(TAutoPtr<IEventHandle> ev);

    template <ESendingType SendingType>
    bool TActivationContext::Send(TAutoPtr<IEventHandle> ev) {
        return TlsActivationContext->ExecutorThread.Send<SendingType>(ev);
    }

    template bool TActivationContext::Send<ESendingType::Common>(std::unique_ptr<IEventHandle> &&ev);
    template bool TActivationContext::Send<ESendingType::Lazy>(std::unique_ptr<IEventHandle> &&ev);
    template bool TActivationContext::Send<ESendingType::Tail>(std::unique_ptr<IEventHandle> &&ev);

    template <ESendingType SendingType>
    bool TActivationContext::Send(std::unique_ptr<IEventHandle> &&ev) {
        return TlsActivationContext->ExecutorThread.Send<SendingType>(ev.release());
    }

    template bool TActivationContext::Forward<ESendingType::Common>(TAutoPtr<IEventHandle>& ev, const TActorId& recipient);
    template bool TActivationContext::Forward<ESendingType::Lazy>(TAutoPtr<IEventHandle>& ev, const TActorId& recipient);
    template bool TActivationContext::Forward<ESendingType::Tail>(TAutoPtr<IEventHandle>& ev, const TActorId& recipient);

    template <ESendingType SendingType>
    bool TActivationContext::Forward(TAutoPtr<IEventHandle>& ev, const TActorId& recipient) {
        return Send(IEventHandle::Forward(ev, recipient));
    }

    template bool TActivationContext::Forward<ESendingType::Common>(THolder<IEventHandle>& ev, const TActorId& recipient);
    template bool TActivationContext::Forward<ESendingType::Lazy>(THolder<IEventHandle>& ev, const TActorId& recipient);
    template bool TActivationContext::Forward<ESendingType::Tail>(THolder<IEventHandle>& ev, const TActorId& recipient);

    template <ESendingType SendingType>
    bool TActivationContext::Forward(THolder<IEventHandle>& ev, const TActorId& recipient) {
        return Send(IEventHandle::Forward(ev, recipient));
    }

    template bool TActorContext::Send<ESendingType::Common>(const TActorId& recipient, IEventBase* ev, TEventFlags flags, ui64 cookie, NWilson::TTraceId traceId) const;
    template bool TActorContext::Send<ESendingType::Lazy>(const TActorId& recipient, IEventBase* ev, TEventFlags flags, ui64 cookie, NWilson::TTraceId traceId) const;
    template bool TActorContext::Send<ESendingType::Tail>(const TActorId& recipient, IEventBase* ev, TEventFlags flags, ui64 cookie, NWilson::TTraceId traceId) const;

    template <ESendingType SendingType>
    bool TActorContext::Send(const TActorId& recipient, IEventBase* ev, TEventFlags flags, ui64 cookie, NWilson::TTraceId traceId) const {
        return Send<SendingType>(new IEventHandle(recipient, SelfID, ev, flags, cookie, nullptr, std::move(traceId)));
    }

    template bool TActorContext::Send<ESendingType::Common>(TAutoPtr<IEventHandle> ev) const;
    template bool TActorContext::Send<ESendingType::Lazy>(TAutoPtr<IEventHandle> ev) const;
    template bool TActorContext::Send<ESendingType::Tail>(TAutoPtr<IEventHandle> ev) const;

    template <ESendingType SendingType>
    bool TActorContext::Send(TAutoPtr<IEventHandle> ev) const {
        return ExecutorThread.Send<SendingType>(ev);
    }

    template bool TActorContext::Forward<ESendingType::Common>(TAutoPtr<IEventHandle>& ev, const TActorId& recipient) const;
    template bool TActorContext::Forward<ESendingType::Lazy>(TAutoPtr<IEventHandle>& ev, const TActorId& recipient) const;
    template bool TActorContext::Forward<ESendingType::Tail>(TAutoPtr<IEventHandle>& ev, const TActorId& recipient) const;

    template <ESendingType SendingType>
    bool TActorContext::Forward(TAutoPtr<IEventHandle>& ev, const TActorId& recipient) const {
        return ExecutorThread.Send<SendingType>(IEventHandle::Forward(ev, recipient));
    }

    template bool TActorContext::Forward<ESendingType::Common>(THolder<IEventHandle>& ev, const TActorId& recipient) const;
    template bool TActorContext::Forward<ESendingType::Lazy>(THolder<IEventHandle>& ev, const TActorId& recipient) const;
    template bool TActorContext::Forward<ESendingType::Tail>(THolder<IEventHandle>& ev, const TActorId& recipient) const;

    template <ESendingType SendingType>
    bool TActorContext::Forward(THolder<IEventHandle>& ev, const TActorId& recipient) const {
        return ExecutorThread.Send<SendingType>(IEventHandle::Forward(ev, recipient));
    }

    template TActorId TActivationContext::Register<ESendingType::Common>(IActor* actor, TActorId parentId, TMailboxType::EType mailboxType, ui32 poolId);
    template TActorId TActivationContext::Register<ESendingType::Lazy>(IActor* actor, TActorId parentId, TMailboxType::EType mailboxType, ui32 poolId);
    template TActorId TActivationContext::Register<ESendingType::Tail>(IActor* actor, TActorId parentId, TMailboxType::EType mailboxType, ui32 poolId);

    template <ESendingType SendingType>
    TActorId TActivationContext::Register(IActor* actor, TActorId parentId, TMailboxType::EType mailboxType, ui32 poolId) {
        return TlsActivationContext->ExecutorThread.RegisterActor<SendingType>(actor, mailboxType, poolId, parentId);
    }

    template TActorId TActorContext::Register<ESendingType::Common>(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId) const;
    template TActorId TActorContext::Register<ESendingType::Lazy>(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId) const;
    template TActorId TActorContext::Register<ESendingType::Tail>(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId) const;

    template <ESendingType SendingType>
    TActorId TActorContext::Register(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId) const {
        return ExecutorThread.RegisterActor<SendingType>(actor, mailboxType, poolId, SelfID);
    }

    template bool TActorIdentity::Send<ESendingType::Common>(const TActorId& recipient, IEventBase* ev, TEventFlags flags, ui64 cookie, NWilson::TTraceId traceId) const;
    template bool TActorIdentity::Send<ESendingType::Lazy>(const TActorId& recipient, IEventBase* ev, TEventFlags flags, ui64 cookie, NWilson::TTraceId traceId) const;
    template bool TActorIdentity::Send<ESendingType::Tail>(const TActorId& recipient, IEventBase* ev, TEventFlags flags, ui64 cookie, NWilson::TTraceId traceId) const;

    template <ESendingType SendingType>
    bool TActorIdentity::Send(const TActorId& recipient, IEventBase* ev, TEventFlags flags, ui64 cookie, NWilson::TTraceId traceId) const {
        return TActivationContext::Send<SendingType>(new IEventHandle(recipient, *this, ev, flags, cookie, nullptr, std::move(traceId)));
    }

    template bool IActor::Send<ESendingType::Common>(const TActorId& recipient, IEventBase* ev, TEventFlags flags, ui64 cookie, NWilson::TTraceId traceId) const;
    template bool IActor::Send<ESendingType::Lazy>(const TActorId& recipient, IEventBase* ev, TEventFlags flags, ui64 cookie, NWilson::TTraceId traceId) const;
    template bool IActor::Send<ESendingType::Tail>(const TActorId& recipient, IEventBase* ev, TEventFlags flags, ui64 cookie, NWilson::TTraceId traceId) const;

    template <ESendingType SendingType>
    bool IActor::Send(const TActorId& recipient, IEventBase* ev, TEventFlags flags, ui64 cookie, NWilson::TTraceId traceId) const {
        return SelfActorId.Send<SendingType>(recipient, ev, flags, cookie, std::move(traceId));
    }

    template TActorId IActor::Register<ESendingType::Common>(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId) const noexcept;
    template TActorId IActor::Register<ESendingType::Lazy>(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId) const noexcept;
    template TActorId IActor::Register<ESendingType::Tail>(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId) const noexcept;

    template <ESendingType SendingType>
    TActorId IActor::Register(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId) const noexcept {
        Y_ABORT_UNLESS(actor);
        return TlsActivationContext->ExecutorThread.RegisterActor<SendingType>(actor, mailboxType, poolId, SelfActorId);
    }

    template TActorId TActorSystem::Register<ESendingType::Common>(IActor* actor, TMailboxType::EType mailboxType, ui32 executorPool,
                        ui64 revolvingCounter, const TActorId& parentId);
    template TActorId TActorSystem::Register<ESendingType::Lazy>(IActor* actor, TMailboxType::EType mailboxType, ui32 executorPool,
                        ui64 revolvingCounter, const TActorId& parentId);
    template TActorId TActorSystem::Register<ESendingType::Tail>(IActor* actor, TMailboxType::EType mailboxType, ui32 executorPool,
                        ui64 revolvingCounter, const TActorId& parentId);

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
            ESendingType previousType = TlsThreadContext->ExchangeSendingType(SendingType);
            TActorId id = CpuManager->GetExecutorPool(executorPool)->Register(actor, mailboxType, revolvingCounter, parentId);
            TlsThreadContext->SetSendingType(previousType);
            return id;
        }
    }

    template bool TActorSystem::Send<ESendingType::Common>(TAutoPtr<IEventHandle> ev) const;
    template bool TActorSystem::Send<ESendingType::Lazy>(TAutoPtr<IEventHandle> ev) const;
    template bool TActorSystem::Send<ESendingType::Tail>(TAutoPtr<IEventHandle> ev) const;

    template <ESendingType SendingType>
    bool TActorSystem::Send(TAutoPtr<IEventHandle> ev) const {
        if constexpr (SendingType == ESendingType::Common) {
            return this->GenericSend< &IExecutorPool::Send>(std::unique_ptr<IEventHandle>(ev.Release()));
        } else {
            return this->SpecificSend(std::unique_ptr<IEventHandle>(ev.Release()), SendingType);
        }
    }

    template bool TActorSystem::Send<ESendingType::Common>(std::unique_ptr<IEventHandle>&& ev) const;
    template bool TActorSystem::Send<ESendingType::Lazy>(std::unique_ptr<IEventHandle>&& ev) const;
    template bool TActorSystem::Send<ESendingType::Tail>(std::unique_ptr<IEventHandle>&& ev) const;

    template <ESendingType SendingType>
    bool TActorSystem::Send(std::unique_ptr<IEventHandle>&& ev) const {
        if constexpr (SendingType == ESendingType::Common) {
            return this->GenericSend< &IExecutorPool::Send>(std::move(ev));
        } else {
            return this->SpecificSend(std::move(ev), SendingType);
        }
    }

    ui32 TActivationContext::GetOverwrittenEventsPerMailbox() {
        return TlsActivationContext->ExecutorThread.GetOverwrittenEventsPerMailbox();
    }

    void TActivationContext::SetOverwrittenEventsPerMailbox(ui32 value) {
        TlsActivationContext->ExecutorThread.SetOverwrittenEventsPerMailbox(value);
    }

    ui64 TActivationContext::GetOverwrittenTimePerMailboxTs() {
        return TlsActivationContext->ExecutorThread.GetOverwrittenTimePerMailboxTs();
    }

    void TActivationContext::SetOverwrittenTimePerMailboxTs(ui64 value) {
        TlsActivationContext->ExecutorThread.SetOverwrittenTimePerMailboxTs(value);
    }
}

template <>
void Out<NActors::TActorActivityType>(IOutputStream& o, const NActors::TActorActivityType& x) {
    o << x.GetName();
}
