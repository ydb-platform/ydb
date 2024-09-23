#include "actor.h"
#include "actor_virtual.h"
#include "actorsystem.h"
#include "executor_thread.h"
#include <ydb/library/actors/util/datetime.h>

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

    template<i64 Increment>
    static void UpdateQueueSizeAndTimestamp(TActorUsageImpl<true>& impl, ui64 time) {
        ui64 usedTimeIncrement = 0;
        using T = TActorUsageImpl<true>;

        for (;;) {
            uint64_t value = impl.QueueSizeAndTimestamp.load();
            ui64 count = value >> T::TimestampBits;

            count += Increment;
            Y_ABORT_UNLESS((count & ~T::CountMask) == 0);

            ui64 timestamp = value;
            if (Increment == 1 && count == 1) {
                timestamp = time;
            } else if (Increment == -1 && count == 0) {
                usedTimeIncrement = (static_cast<ui64>(time) - timestamp) & T::TimestampMask;
                timestamp = 0; // reset timestamp to some zero value
            }

            const ui64 updated = (timestamp & T::TimestampMask) | (count << T::TimestampBits);
            if (impl.QueueSizeAndTimestamp.compare_exchange_weak(value, updated)) {
                break;
            }
        }

        if (usedTimeIncrement && impl.LastUsageTimestamp <= time) {
            impl.UsedTime += usedTimeIncrement;
        }
    }

    void TActorUsageImpl<true>::OnEnqueueEvent(ui64 time) {
        UpdateQueueSizeAndTimestamp<+1>(*this, time);
    }

    void TActorUsageImpl<true>::OnDequeueEvent() {
        UpdateQueueSizeAndTimestamp<-1>(*this, GetCycleCountFast());
    }

    double TActorUsageImpl<true>::GetUsage(ui64 time) {
        ui64 used = UsedTime.exchange(0);
        if (const ui64 value = QueueSizeAndTimestamp.load(); value >> TimestampBits) {
            used += (static_cast<ui64>(time) - value) & TimestampMask;
        }

        Y_ABORT_UNLESS(LastUsageTimestamp <= time);
        ui64 passed = time - LastUsageTimestamp;
        LastUsageTimestamp = time;

        if (!passed) {
            return 0;
        }

        return (double)Min(passed, used) / passed;
    }

    void IActor::Describe(IOutputStream &out) const noexcept {
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
        return ctx.ExecutorThread.RegisterActor(actor, &ctx.Mailbox, parentId.Hint(), parentId);
    }

    TActorId TActorContext::RegisterWithSameMailbox(IActor* actor) const {
        return ExecutorThread.RegisterActor(actor, &Mailbox, SelfID.Hint(), SelfID);
    }

    TActorId IActor::RegisterWithSameMailbox(IActor* actor) const noexcept {
        return TlsActivationContext->ExecutorThread.RegisterActor(actor, &TlsActivationContext->Mailbox, SelfActorId.Hint(), SelfActorId);
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

    void IActor::PassAway() {
        auto& cx = *TlsActivationContext;
        cx.ExecutorThread.UnregisterActor(&cx.Mailbox, SelfActorId);
    }

    double IActor::GetElapsedTicksAsSeconds() const {
        return NHPTimer::GetSeconds(ElapsedTicks);
    }

    void TActorCallbackBehaviour::Receive(IActor* actor, TAutoPtr<IEventHandle>& ev) {
        (actor->*StateFunc)(ev);
    }

    void TActorVirtualBehaviour::Receive(IActor* actor, std::unique_ptr<IEventHandle> ev) {
        Y_ABORT_UNLESS(!!ev && ev->GetBase());
        ev->GetBase()->Execute(actor, std::move(ev));
    }

    void IActor::Registered(TActorSystem* sys, const TActorId& owner) {
        // fallback to legacy method, do not use it anymore
        if (auto eh = AfterRegister(SelfId(), owner)) {
            if (!TlsThreadContext || TlsThreadContext->SendingType == ESendingType::Common) {
                sys->Send(eh);
            } else {
                sys->SpecificSend(eh);
            }
        }
    }

    void IActor::SetEnoughCpu(bool isEnough) {
        if (TlsThreadContext) {
            TlsThreadContext->IsEnoughCpu = isEnough;
        }
    }

    template bool TGenericExecutorThread::Send<ESendingType::Common>(TAutoPtr<IEventHandle> ev);
    template bool TGenericExecutorThread::Send<ESendingType::Lazy>(TAutoPtr<IEventHandle> ev);
    template bool TGenericExecutorThread::Send<ESendingType::Tail>(TAutoPtr<IEventHandle> ev);

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

    template TActorId TGenericExecutorThread::RegisterActor<ESendingType::Common>(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId,
            TActorId parentId);
    template TActorId TGenericExecutorThread::RegisterActor<ESendingType::Lazy>(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId,
            TActorId parentId);
    template TActorId TGenericExecutorThread::RegisterActor<ESendingType::Tail>(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId,
            TActorId parentId);

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

    template TActorId TGenericExecutorThread::RegisterActor<ESendingType::Common>(IActor* actor, TMailboxHeader* mailbox, ui32 hint, TActorId parentId);
    template TActorId TGenericExecutorThread::RegisterActor<ESendingType::Lazy>(IActor* actor, TMailboxHeader* mailbox, ui32 hint, TActorId parentId);
    template TActorId TGenericExecutorThread::RegisterActor<ESendingType::Tail>(IActor* actor, TMailboxHeader* mailbox, ui32 hint, TActorId parentId);

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
            ESendingType previousType = std::exchange(TlsThreadContext->SendingType, SendingType);
            TActorId id = CpuManager->GetExecutorPool(executorPool)->Register(actor, mailboxType, revolvingCounter, parentId);
            TlsThreadContext->SendingType = previousType;
            return id;
        }
    }

    template bool TActorSystem::Send<ESendingType::Common>(TAutoPtr<IEventHandle> ev) const;
    template bool TActorSystem::Send<ESendingType::Lazy>(TAutoPtr<IEventHandle> ev) const;
    template bool TActorSystem::Send<ESendingType::Tail>(TAutoPtr<IEventHandle> ev) const;

    template <ESendingType SendingType>
    bool TActorSystem::Send(TAutoPtr<IEventHandle> ev) const {
        if constexpr (SendingType == ESendingType::Common) {
            return this->GenericSend< &IExecutorPool::Send>(ev);
        } else {
            return this->SpecificSend(ev, SendingType);
        }
    }
}
